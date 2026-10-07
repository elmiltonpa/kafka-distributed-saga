import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import {
  ActivationRequestedEvent,
  BillingAccountCreatedEvent,
  BillingFailedEvent,
  TOPICS,
} from '@poc/event-contracts';

@Injectable()
export class BillingService implements OnModuleInit {
  private readonly logger = new Logger(BillingService.name);

  private readonly accounts = new Map<
    string,
    { billingAccountId: string; status: string }
  >();

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    this.logger.log('Conectando Kafka producer en billing-service...');
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer de billing-service conectado con exito.');
  }

  async processActivationRequested(event: ActivationRequestedEvent) {
    const { correlationId, customerId, payload } = event;
    const now = new Date().toISOString();

    this.logger.log(
      `[billing-service] Procesando ActivationRequested para cliente [${customerId}], activationId [${correlationId}]`,
    );

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
}
