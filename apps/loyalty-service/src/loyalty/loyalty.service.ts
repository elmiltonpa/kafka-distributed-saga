import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { EventEnvelope, TOPICS } from '@poc/event-contracts';
import { DlqService, IdempotencyService, MongoClientService } from '@poc/kafka-toolkit';

export interface CustomerLoyalty {
  customerId: string;
  points: number;
  activations: string[];
  updatedAt: string;
}

@Injectable()
export class LoyaltyService {
  private readonly logger = new Logger(LoyaltyService.name);
  private readonly loyaltyMap = new Map<string, CustomerLoyalty>();

  constructor(
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqService: DlqService,
    private readonly mongoService: MongoClientService,
    @Optional() @Inject('KAFKA_CLIENT') private readonly kafkaClient?: ClientKafka,
  ) {}

  private async persistLoyalty(record: CustomerLoyalty) {
    this.loyaltyMap.set(record.customerId, record);
    const db = this.mongoService.getDb();
    if (db) {
      try {
        await db.collection('loyalty_points').updateOne(
          { customerId: record.customerId },
          { $set: record },
          { upsert: true },
        );
      } catch (err) {
        this.logger.warn(`Error al persistir puntos de fidelidad en Mongo: ${err}`);
      }
    }
  }

  async processActivationEvent(event: EventEnvelope<any>) {
    await this.dlqService.executeWithRetry(TOPICS.ACTIVATION_EVENTS, event, async () => {
      const { eventId, eventType, customerId, correlationId } = event;

      if (eventType !== 'ActivationCompleted') {
        return;
      }

      const alreadyProcessed = await this.idempotencyService.isAlreadyProcessed(eventId, 'loyalty-service');
      if (alreadyProcessed) {
        this.logger.warn(
          `🛡️ [Idempotencia] Replay: Evento [${eventId}] ya contabilizado para loyalty. Descartando duplicado.`,
        );
        return;
      }

      const existing = this.loyaltyMap.get(customerId) || {
        customerId,
        points: 0,
        activations: [],
        updatedAt: new Date().toISOString(),
      };

      existing.points += 100;
      existing.activations.push(correlationId);
      existing.updatedAt = new Date().toISOString();

      await this.persistLoyalty(existing);

      this.logger.log(
        `🎁 [Loyalty Service - Log Replay] Cliente [${customerId}] sumó 100 puntos por activación [${correlationId}]. Puntos acumulados: ${existing.points}`,
      );
    }, this.kafkaClient);
  }

  getLoyaltyStatus(customerId: string): CustomerLoyalty | undefined {
    return this.loyaltyMap.get(customerId);
  }
}
