import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import {
  ActivationRequestedEvent,
  ProvisioningCompletedEvent,
  ProvisioningFailedEvent,
  TOPICS,
} from '@poc/event-contracts';
import { DlqService, IdempotencyService, MongoClientService } from '@poc/kafka-toolkit';

@Injectable()
export class ProvisioningService implements OnModuleInit {
  private readonly logger = new Logger(ProvisioningService.name);

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqService: DlqService,
    private readonly mongoService: MongoClientService,
  ) {}

  async onModuleInit() {
    this.logger.log('Conectando Kafka producer en provisioning-service...');
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer de provisioning-service conectado con éxito.');
  }

  private async persistOrder(activationId: string, customerId: string, planId: string, status: string, reason?: string) {
    const db = this.mongoService.getDb();
    if (db) {
      try {
        await db.collection('provisioning_orders').updateOne(
          { activationId },
          { $set: { activationId, customerId, planId, status, reason, updatedAt: new Date() } },
          { upsert: true },
        );
      } catch (err) {
        this.logger.warn(`Error al persistir orden en Mongo: ${err}`);
      }
    }
  }

  async processActivationRequested(event: ActivationRequestedEvent) {
    await this.dlqService.executeWithRetry(
      TOPICS.ACTIVATION_REQUESTED,
      event,
      async () => {
      const { eventId, correlationId, customerId, payload } = event;
      const now = new Date().toISOString();

      const alreadyProcessed = await this.idempotencyService.isAlreadyProcessed(eventId, 'provisioning-service');
      if (alreadyProcessed) {
        this.logger.warn(
          `🛡️ [Idempotencia] Evento [${eventId}] ya fue procesado en provisioning. Descartando duplicado.`,
        );
        return;
      }

      this.logger.log(
        `[provisioning-service] Iniciando aprovisionamiento para cliente [${customerId}], plan [${payload.planId}] (activationId=${correlationId})...`,
      );

      await new Promise((resolve) => setTimeout(resolve, 1500));

      if (payload.simulateFailure === 'provisioning') {
        this.logger.warn(
          `[provisioning-service] Simulando FALLO forzado de aprovisionamiento para activationId [${correlationId}]`,
        );

        await this.persistOrder(
          correlationId,
          customerId,
          payload.planId,
          'FAILED',
          'Error técnico al aprovisionar la línea en la red física (Simulado)',
        );

        const failedEvent: ProvisioningFailedEvent = {
          eventId: randomUUID(),
          eventType: 'ProvisioningFailed',
          version: 1,
          occurredAt: now,
          correlationId,
          customerId,
          source: 'provisioning-service',
          payload: {
            reason: 'Error técnico al aprovisionar la línea en la red física (Simulado)',
          },
        };

        this.kafkaClient.emit(TOPICS.PROVISIONING_EVENTS, {
          key: customerId,
          value: failedEvent,
        });

        return;
      }

      await this.persistOrder(correlationId, customerId, payload.planId, 'COMPLETED');

      this.logger.log(
        `[provisioning-service] Aprovisionamiento completado exitosamente para activationId [${correlationId}]`,
      );

      const completedEvent: ProvisioningCompletedEvent = {
        eventId: randomUUID(),
        eventType: 'ProvisioningCompleted',
        version: 1,
        occurredAt: now,
        correlationId,
        customerId,
        source: 'provisioning-service',
        payload: {
          planId: payload.planId,
        },
      };

      this.kafkaClient.emit(TOPICS.PROVISIONING_EVENTS, {
        key: customerId,
        value: completedEvent,
      });
    }, this.kafkaClient);
  }
}
