import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import {
  ActivationRequestedEvent,
  BillingAccountCancelledEvent,
  BillingAccountCreatedEvent,
  BillingFailedEvent,
  EventEnvelope,
  TOPICS,
} from '@poc/event-contracts';
import { DlqService, IdempotencyService, MongoClientService } from '@poc/kafka-toolkit';

@Injectable()
export class BillingService implements OnModuleInit {
  private readonly logger = new Logger(BillingService.name);
  private readonly accounts = new Map<
    string,
    { billingAccountId: string; status: string }
  >();
  private readonly failedActivations = new Set<string>();

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqService: DlqService,
    private readonly mongoService: MongoClientService,
  ) {}

  async onModuleInit() {
    this.logger.log('Conectando Kafka producer en billing-service...');
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer de billing-service conectado con éxito.');
  }

  private async persistAccount(activationId: string, customerId: string, billingAccountId: string, status: string) {
    this.accounts.set(activationId, { billingAccountId, status });
    const db = this.mongoService.getDb();
    if (db) {
      try {
        await db.collection('billing_accounts').updateOne(
          { activationId },
          { $set: { activationId, customerId, billingAccountId, status, updatedAt: new Date() } },
          { upsert: true },
        );
      } catch (err) {
        this.logger.warn(`Error al persistir cuenta en Mongo: ${err}`);
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

      const alreadyProcessed = await this.idempotencyService.isAlreadyProcessed(eventId, 'billing-service');
      if (alreadyProcessed) {
        this.logger.warn(
          `🛡️ [Idempotencia] Evento [${eventId}] ya fue procesado en billing. Descartando duplicado.`,
        );
        return;
      }

      this.logger.log(
        `[billing-service] Procesando ActivationRequested para cliente [${customerId}], activationId [${correlationId}]`,
      );

      if (this.failedActivations.has(correlationId)) {
        const billingAccountId = `bill-${randomUUID().slice(0, 8)}`;
        await this.persistAccount(correlationId, customerId, billingAccountId, 'CANCELLED');

        this.logger.warn(
          `🔄 [billing-service] CASO BORDE: ActivationFailed llegó antes. Creando cuenta [${billingAccountId}] como CANCELLED.`,
        );

        const cancelledEvent: BillingAccountCancelledEvent = {
          eventId: randomUUID(),
          eventType: 'BillingAccountCancelled',
          version: 1,
          occurredAt: now,
          correlationId,
          customerId,
          source: 'billing-service',
          payload: { billingAccountId },
        };

        this.kafkaClient.emit(TOPICS.BILLING_EVENTS, {
          key: customerId,
          value: cancelledEvent,
        });
        return;
      }

      if (payload.simulateFailure === 'billing') {
        this.logger.warn(
          `[billing-service] Simulando FALLO forzado de facturación para activationId [${correlationId}]`,
        );

        const failedEvent: BillingFailedEvent = {
          eventId: randomUUID(),
          eventType: 'BillingFailed',
          version: 1,
          occurredAt: now,
          correlationId,
          customerId,
          source: 'billing-service',
          payload: {
            reason: 'Fallo simulado en pasarela de facturación',
          },
        };

        this.kafkaClient.emit(TOPICS.BILLING_EVENTS, {
          key: customerId,
          value: failedEvent,
        });
        return;
      }

      const billingAccountId = `bill-${randomUUID().slice(0, 8)}`;
      await this.persistAccount(correlationId, customerId, billingAccountId, 'CREATED');

      this.logger.log(
        `[billing-service] Cuenta creada con éxito: [${billingAccountId}] para activationId [${correlationId}]`,
      );

      const createdEvent: BillingAccountCreatedEvent = {
        eventId: randomUUID(),
        eventType: 'BillingAccountCreated',
        version: 1,
        occurredAt: now,
        correlationId,
        customerId,
        source: 'billing-service',
        payload: { billingAccountId },
      };

      this.kafkaClient.emit(TOPICS.BILLING_EVENTS, {
        key: customerId,
        value: createdEvent,
      });
    }, this.kafkaClient);
  }

  async processActivationFailed(event: EventEnvelope<any>) {
    await this.dlqService.executeWithRetry(
      TOPICS.ACTIVATION_EVENTS,
      event,
      async () => {
      const { eventId, correlationId, customerId } = event;

      const alreadyProcessed = await this.idempotencyService.isAlreadyProcessed(eventId, 'billing-service');
      if (alreadyProcessed) {
        this.logger.warn(
          `🛡️ [Idempotencia] Evento de compensación [${eventId}] ya procesado. Descartando.`,
        );
        return;
      }

      this.failedActivations.add(correlationId);

      const account = this.accounts.get(correlationId);
      if (!account) {
        this.logger.log(
          `[billing-service] Compensación: Aún no existe cuenta para activationId [${correlationId}]. Registrada para auto-cancelación.`,
        );
        return;
      }

      if (account.status === 'CANCELLED') {
        return;
      }

      await this.persistAccount(correlationId, customerId, account.billingAccountId, 'CANCELLED');
      this.logger.warn(
        `🔄 [billing-service] COMPENSACIÓN EJECUTADA: Cuenta [${account.billingAccountId}] anulada para activationId [${correlationId}]`,
      );

      const cancelledEvent: BillingAccountCancelledEvent = {
        eventId: randomUUID(),
        eventType: 'BillingAccountCancelled',
        version: 1,
        occurredAt: new Date().toISOString(),
        correlationId,
        customerId,
        source: 'billing-service',
        payload: {
          billingAccountId: account.billingAccountId,
        },
      };

      this.kafkaClient.emit(TOPICS.BILLING_EVENTS, {
        key: customerId,
        value: cancelledEvent,
      });
    }, this.kafkaClient);
  }
}
