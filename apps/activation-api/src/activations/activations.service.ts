import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { randomUUID } from 'crypto';
import {
  ActivationCompletedEvent,
  ActivationFailedEvent,
  ActivationRequestedEvent,
  EventEnvelope,
  TOPICS,
} from '@poc/event-contracts';
import { CreateActivationDto } from './dto/create-activation.dto';

export interface ActivationRecord {
  activationId: string;
  customerId: string;
  planId: string;
  status: 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
  simulateFailure: 'none' | 'billing' | 'provisioning';
  createdAt: string;
  updatedAt: string;
  steps: {
    billing?: {
      status: string;
      at: string;
      reason?: string;
      billingAccountId?: string;
    };
    provisioning?: { status: string; at: string; reason?: string };
  };
  history: Array<{ eventType: string; at: string }>;
}

@Injectable()
export class ActivationsService implements OnModuleInit {
  private readonly logger = new Logger(ActivationsService.name);

  private readonly activations = new Map<string, ActivationRecord>();

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit() {
    this.logger.log('Conectando Kafka producer en activation-api...');
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer conectado exitosamente.');
  }

  async requestActivation(dto: CreateActivationDto): Promise<ActivationRecord> {
    const activationId = `act-${randomUUID().slice(0, 8)}`;
    const now = new Date().toISOString();

    const record: ActivationRecord = {
      activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      status: 'PENDING',
      simulateFailure: dto.simulateFailure || 'none',
      createdAt: now,
      updatedAt: now,
      steps: {},
      history: [
        {
          eventType: 'ActivationRequested',
          at: now,
        },
      ],
    };

    this.activations.set(activationId, record);

    const event: ActivationRequestedEvent = {
      eventId: randomUUID(),
      eventType: 'ActivationRequested',
      version: 1,
      occurredAt: now,
      correlationId: activationId,
      customerId: dto.customerId,
      source: 'activation-api',
      payload: {
        planId: dto.planId,
        channel: 'web',
        simulateFailure: dto.simulateFailure || 'none',
      },
    };

    this.logger.log(
      `[Saga Agregador] Publicando ActivationRequested en topico "${TOPICS.ACTIVATION_REQUESTED}" [key=${dto.customerId}, activationId=${activationId}]`,
    );

    this.kafkaClient.emit(TOPICS.ACTIVATION_REQUESTED, {
      key: dto.customerId,
      value: event,
    });

    return record;
  }

  async handleBillingEvent(event: EventEnvelope<any>) {
    const { correlationId, eventType, occurredAt, payload } = event;
    const record = this.activations.get(correlationId);

    if (!record) {
      this.logger.warn(
        `[Agregador] Evento de billing ignorado: activationId [${correlationId}] no existe.`,
      );
      return;
    }

    this.logger.log(
      `[Agregador] Recibido evento de billing: ${eventType} para activationId [${correlationId}]`,
    );

    if (eventType === 'BillingAccountCreated') {
      record.steps.billing = {
        status: 'OK',
        at: occurredAt,
        billingAccountId: payload.billingAccountId,
      };
    } else if (eventType === 'BillingFailed') {
      record.steps.billing = {
        status: 'FAILED',
        at: occurredAt,
        reason: payload.reason,
      };
    }

    record.history.push({ eventType, at: occurredAt });
    this.evaluateActivationStatus(record);
  }

  async handleProvisioningEvent(event: EventEnvelope<any>) {
    const { correlationId, eventType, occurredAt, payload } = event;
    const record = this.activations.get(correlationId);

    if (!record) {
      this.logger.warn(
        `[Agregador] Evento de provisioning ignorado: activationId [${correlationId}] no existe.`,
      );
      return;
    }

    this.logger.log(
      `[Agregador] Recibido evento de provisioning: ${eventType} para activationId [${correlationId}]`,
    );

    if (eventType === 'ProvisioningCompleted') {
      record.steps.provisioning = {
        status: 'OK',
        at: occurredAt,
      };
    } else if (eventType === 'ProvisioningFailed') {
      record.steps.provisioning = {
        status: 'FAILED',
        at: occurredAt,
        reason: payload.reason,
      };
    }

    record.history.push({ eventType, at: occurredAt });
    this.evaluateActivationStatus(record);
  }

  private evaluateActivationStatus(record: ActivationRecord) {
    if (record.status === 'ACTIVE' || record.status === 'FAILED') {
      return;
    }

    if (record.status === 'PENDING') {
      record.status = 'IN_PROGRESS';
      record.updatedAt = new Date().toISOString();
    }

    const billingStatus = record.steps.billing?.status;
    const provisioningStatus = record.steps.provisioning?.status;

    if (billingStatus === 'FAILED' || provisioningStatus === 'FAILED') {
      record.status = 'FAILED';
      record.updatedAt = new Date().toISOString();

      const failureReason =
        record.steps.billing?.reason ||
        record.steps.provisioning?.reason ||
        'Fallo en uno de los servicios';

      this.logger.error(
        `🚨 [Saga Agregador] Activacion fallida para [${record.activationId}]. Motivo: ${failureReason}`,
      );

      const failedEvent: ActivationFailedEvent = {
        eventId: randomUUID(),
        eventType: 'ActivationFailed',
        version: 1,
        occurredAt: record.updatedAt,
        correlationId: record.activationId,
        customerId: record.customerId,
        source: 'activation-api',
        payload: {
          reason: failureReason,
        },
      };

      record.history.push({
        eventType: 'ActivationFailed',
        at: record.updatedAt,
      });

      this.kafkaClient.emit(TOPICS.ACTIVATION_EVENTS, {
        key: record.customerId,
        value: failedEvent,
      });

      return;
    }

    if (billingStatus === 'OK' && provisioningStatus === 'OK') {
      record.status = 'ACTIVE';
      record.updatedAt = new Date().toISOString();

      this.logger.log(
        `✅ [Saga Agregador] Activacion completada con exito para [${record.activationId}]. Estado -> ACTIVE`,
      );

      const completedEvent: ActivationCompletedEvent = {
        eventId: randomUUID(),
        eventType: 'ActivationCompleted',
        version: 1,
        occurredAt: record.updatedAt,
        correlationId: record.activationId,
        customerId: record.customerId,
        source: 'activation-api',
        payload: {
          planId: record.planId,
        },
      };

      record.history.push({
        eventType: 'ActivationCompleted',
        at: record.updatedAt,
      });

      this.kafkaClient.emit(TOPICS.ACTIVATION_EVENTS, {
        key: record.customerId,
        value: completedEvent,
      });
    }
  }

  getActivation(id: string): ActivationRecord | undefined {
    return this.activations.get(id);
  }

  getAllActivations(): ActivationRecord[] {
    return Array.from(this.activations.values());
  }
}
