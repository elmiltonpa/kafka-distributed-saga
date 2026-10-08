import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import {
  ActivationRequestedEvent,
  ProvisioningCompletedEvent,
  ProvisioningFailedEvent,
  TOPICS,
} from '@poc/event-contracts';

@Injectable()
export class ProvisioningService implements OnModuleInit {
  private readonly logger = new Logger(ProvisioningService.name);

  private readonly processedEvents = new Set<string>();

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    this.logger.log('Conectando Kafka producer en provisioning-service...');
    await this.kafkaClient.connect();
    this.logger.log(
      'Kafka producer de provisioning-service conectado con exito.',
    );
  }

  async processActivationRequested(event: ActivationRequestedEvent) {
    const { eventId, correlationId, customerId, payload } = event;
    const now = new Date().toISOString();

    if (this.processedEvents.has(eventId)) {
      this.logger.warn(
        `🛡️ [Idempotencia] Evento [${eventId}] ya fue procesado en provisioning. Descartando duplicado.`,
      );
      return;
    }
    this.processedEvents.add(eventId);

    this.logger.log(
      `[provisioning-service] Iniciando aprovisionamiento para cliente [${customerId}], plan [${payload.planId}] (activationId=${correlationId})...`,
    );

    await new Promise((resolve) => setTimeout(resolve, 1500));

    if (payload.simulateFailure === 'provisioning') {
      this.logger.warn(
        `[provisioning-service] Simulando FALLO forzado de aprovisionamiento para activationId [${correlationId}]`,
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
          reason:
            'Error técnico al aprovisionar la línea en la red física (Simulado)',
        },
      };

      this.kafkaClient.emit(TOPICS.PROVISIONING_EVENTS, {
        key: customerId,
        value: failedEvent,
      });

      return;
    }

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
  }
}
