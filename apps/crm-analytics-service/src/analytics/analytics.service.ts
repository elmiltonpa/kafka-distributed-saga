import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { EventEnvelope } from '@poc/event-contracts';
import { DlqService, IdempotencyService, MongoClientService } from '@poc/kafka-toolkit';

export interface AuditedEvent {
  eventId: string;
  eventType: string;
  topic: string;
  source: string;
  customerId: string;
  correlationId: string;
  occurredAt: string;
  auditedAt: string;
  payload: unknown;
}

@Injectable()
export class AnalyticsService {
  private readonly logger = new Logger(AnalyticsService.name);
  private readonly eventLog: AuditedEvent[] = [];

  constructor(
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqService: DlqService,
    private readonly mongoService: MongoClientService,
    @Optional() @Inject('KAFKA_CLIENT') private readonly kafkaClient?: ClientKafka,
  ) {}

  private async persistEvent(event: AuditedEvent) {
    this.eventLog.push(event);
    const db = this.mongoService.getDb();
    if (db) {
      try {
        await db.collection('event_log').insertOne(event);
      } catch (err) {
        this.logger.warn(`Error al persistir evento en Mongo: ${err}`);
      }
    }
  }

  async recordEvent(topic: string, event: EventEnvelope<unknown>) {
    await this.dlqService.executeWithRetry(topic, event, async () => {
      const {
        eventId,
        eventType,
        source,
        customerId,
        correlationId,
        occurredAt,
        payload,
      } = event;

      const alreadyProcessed = await this.idempotencyService.isAlreadyProcessed(eventId, 'crm-analytics');
      if (alreadyProcessed) {
        this.logger.warn(
          `🛡️ [Idempotencia] Evento auditado previamente [${eventId}] (${eventType}) descartado.`,
        );
        return;
      }

      const auditedEvent: AuditedEvent = {
        eventId,
        eventType,
        topic,
        source,
        customerId,
        correlationId,
        occurredAt,
        auditedAt: new Date().toISOString(),
        payload,
      };

      await this.persistEvent(auditedEvent);

      this.logger.log(
        `📊 [CRM Analytics Audit] Tópico: "${topic}" | Evento: [${eventType}] | Cliente: [${customerId}] | ActivationId: [${correlationId}] | Fuente: [${source}] (Total auditados: ${this.eventLog.length})`,
      );
    }, this.kafkaClient);
  }

  getAuditLog(): AuditedEvent[] {
    return this.eventLog;
  }

  getEventsByActivation(correlationId: string): AuditedEvent[] {
    return this.eventLog.filter((e) => e.correlationId === correlationId);
  }
}
