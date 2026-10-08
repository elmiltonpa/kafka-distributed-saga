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

@Injectable()
export class BillingService implements OnModuleInit {
  private readonly logger = new Logger(BillingService.name);

  private readonly accounts = new Map<
    string,
    { billingAccountId: string; status: string }
  >();

  private readonly processedEvents = new Set<string>();

  private readonly failedActivations = new Set<string>();

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    this.logger.log('Conectando Kafka producer en billing-service...');
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer de billing-service conectado con exito.');
  }

  async processActivationRequested(event: ActivationRequestedEvent) {
    const { eventId, correlationId, customerId, payload } = event;
    const now = new Date().toISOString();

    if (this.processedEvents.has(eventId)) {
      this.logger.warn(
        `🛡️ [Idempotencia] Evento [${eventId}] ya fue procesado en billing. Descartando duplicado.`,
      );
      return;
    }
    this.processedEvents.add(eventId);

    this.logger.log(
      `[billing-service] Procesando ActivationRequested para cliente [${customerId}], activationId [${correlationId}]`,
    );

    if (this.failedActivations.has(correlationId)) {
      const billingAccountId = `bill-${randomUUID().slice(0, 8)}`;
      this.accounts.set(correlationId, {
        billingAccountId,
        status: 'CANCELLED',
      });

      this.logger.warn(
        `🔄 [billing-service] CASO BORDE RESUELTO: ActivationFailed llegó antes de crear la cuenta. Creando cuenta [${billingAccountId}] directamente como CANCELLED.`,
      );

      const cancelledEvent: BillingAccountCancelledEvent = {
        eventId: randomUUID(),
        eventType: 'BillingAccountCancelled',
        version: 1,
        occurredAt: now,
        correlationId,
        customerId,
        source: 'billing-service',
        payload: {
          billingAccountId,
        },
      };

      this.kafkaClient.emit(TOPICS.BILLING_EVENTS, {
        key: customerId,
        value: cancelledEvent,
      });

      return;
    }

    if (payload.simulateFailure === 'billing') {
      this.logger.warn(
        `[billing-service] Simulando FALLO forzado de facturacion para activationId [${correlationId}]`,
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
    this.accounts.set(correlationId, {
      billingAccountId,
      status: 'CREATED',
    });

    this.logger.log(
      `[billing-service] Cuenta creada con exito: [${billingAccountId}] para activationId [${correlationId}]`,
    );

    const createdEvent: BillingAccountCreatedEvent = {
      eventId: randomUUID(),
      eventType: 'BillingAccountCreated',
      version: 1,
      occurredAt: now,
      correlationId,
      customerId,
      source: 'billing-service',
      payload: {
        billingAccountId,
      },
    };

    this.kafkaClient.emit(TOPICS.BILLING_EVENTS, {
      key: customerId,
      value: createdEvent,
    });
  }

  async processActivationFailed(event: EventEnvelope<any>) {
    const { eventId, correlationId, customerId } = event;

    if (this.processedEvents.has(eventId)) {
      this.logger.warn(
        `🛡️ [Idempotencia] Evento de compensación [${eventId}] ya procesado. Descartando.`,
      );
      return;
    }
    this.processedEvents.add(eventId);

    this.failedActivations.add(correlationId);

    const account = this.accounts.get(correlationId);

    if (!account) {
      this.logger.log(
        `[billing-service] Compensación: Aún no existe cuenta creada para activationId [${correlationId}]. Se registró en failedActivations para auto-cancelarse al crearse.`,
      );
      return;
    }

    if (account.status === 'CANCELLED') {
      this.logger.log(
        `[billing-service] Compensación: La cuenta [${account.billingAccountId}] ya estaba cancelada previamente.`,
      );
      return;
    }

    account.status = 'CANCELLED';
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
  }
}
