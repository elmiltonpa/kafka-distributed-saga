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
import { DlqService, IdempotencyService, MongoClientService } from '@poc/kafka-toolkit';
import { ActivationsGateway } from './activations.gateway';
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
  history: Array<{
    eventType: string;
    at: string;
    details?: Record<string, unknown>;
  }>;
}

@Injectable()
export class ActivationsService implements OnModuleInit {
  private readonly logger = new Logger(ActivationsService.name);
  private readonly activations = new Map<string, ActivationRecord>();

  constructor(
    @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
    private readonly gateway: ActivationsGateway,
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqService: DlqService,
    private readonly mongoService: MongoClientService,
  ) {}

  async onModuleInit() {
    this.logger.log('Conectando Kafka producer en activation-api...');
    await this.kafkaClient.connect();
    this.logger.log('Kafka producer conectado exitosamente.');
  }

  private async persistActivation(record: ActivationRecord) {
    this.activations.set(record.activationId, record);
    const db = this.mongoService.getDb();
    if (db) {
      try {
        await db.collection('activations').updateOne(
          { _id: record.activationId as any },
          { $set: { ...record, _id: record.activationId } },
          { upsert: true },
        );
      } catch (err) {
        this.logger.warn(`Error al persistir activación en Mongo: ${err}`);
      }
    }
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
          details: {
            planId: dto.planId,
            channel: 'web',
            simulateFailure: dto.simulateFailure || 'none',
          },
        },
      ],
    };

    await this.persistActivation(record);

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
      `[Saga Agregador] Publicando ActivationRequested en tópico "${TOPICS.ACTIVATION_REQUESTED}" [key=${dto.customerId}, activationId=${activationId}]`,
    );

    this.kafkaClient.emit(TOPICS.ACTIVATION_REQUESTED, {
      key: dto.customerId,
      value: event,
    });

    this.gateway.emitActivationUpdate({
      activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      eventType: 'ActivationRequested',
      status: 'PENDING',
      details: {
        planId: dto.planId,
        channel: 'web',
        simulateFailure: dto.simulateFailure || 'none',
      },
      timestamp: now,
    });

    return record;
  }

  async handleBillingEvent(event: EventEnvelope<any>) {
    await this.dlqService.executeWithRetry(TOPICS.BILLING_EVENTS, event, async () => {
      const { eventId, correlationId, eventType, occurredAt, payload } = event;

      const alreadyProcessed = await this.idempotencyService.isAlreadyProcessed(eventId, 'activation-api');
      if (alreadyProcessed) {
        this.logger.warn(
          `🛡️ [Idempotencia] Evento de billing [${eventId}] (${eventType}) ya procesado en agregador. Descartando.`,
        );
        return;
      }

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

      record.history.push({ eventType, at: occurredAt, details: payload });

      this.gateway.emitActivationUpdate({
        activationId: correlationId,
        customerId: record.customerId,
        planId: record.planId,
        eventType,
        status: record.status === 'PENDING' ? 'IN_PROGRESS' : record.status,
        details: payload,
        timestamp: occurredAt,
      });

      await this.evaluateActivationStatus(record);
    }, this.kafkaClient);
  }

  async handleProvisioningEvent(event: EventEnvelope<any>) {
    await this.dlqService.executeWithRetry(TOPICS.PROVISIONING_EVENTS, event, async () => {
      const { eventId, correlationId, eventType, occurredAt, payload } = event;

      const alreadyProcessed = await this.idempotencyService.isAlreadyProcessed(eventId, 'activation-api');
      if (alreadyProcessed) {
        this.logger.warn(
          `🛡️ [Idempotencia] Evento de provisioning [${eventId}] (${eventType}) ya procesado en agregador. Descartando.`,
        );
        return;
      }

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

      record.history.push({ eventType, at: occurredAt, details: payload });

      this.gateway.emitActivationUpdate({
        activationId: correlationId,
        customerId: record.customerId,
        planId: record.planId,
        eventType,
        status: record.status === 'PENDING' ? 'IN_PROGRESS' : record.status,
        details: payload,
        timestamp: occurredAt,
      });

      await this.evaluateActivationStatus(record);
    }, this.kafkaClient);
  }

  private async evaluateActivationStatus(record: ActivationRecord) {
    if (record.status === 'ACTIVE' || record.status === 'FAILED') {
      await this.persistActivation(record);
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
        `🚨 [Saga Agregador] Activación fallida para [${record.activationId}]. Motivo: ${failureReason}`,
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
        details: { reason: failureReason },
      });

      await this.persistActivation(record);

      this.kafkaClient.emit(TOPICS.ACTIVATION_EVENTS, {
        key: record.customerId,
        value: failedEvent,
      });

      this.gateway.emitActivationUpdate({
        activationId: record.activationId,
        customerId: record.customerId,
        planId: record.planId,
        eventType: 'ActivationFailed',
        status: 'FAILED',
        details: { reason: failureReason },
        timestamp: record.updatedAt,
      });

      return;
    }

    if (billingStatus === 'OK' && provisioningStatus === 'OK') {
      record.status = 'ACTIVE';
      record.updatedAt = new Date().toISOString();

      this.logger.log(
        `✅ [Saga Agregador] Activación completada con éxito para [${record.activationId}]. Estado -> ACTIVE`,
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
        details: { planId: record.planId },
      });

      await this.persistActivation(record);

      this.kafkaClient.emit(TOPICS.ACTIVATION_EVENTS, {
        key: record.customerId,
        value: completedEvent,
      });

      this.gateway.emitActivationUpdate({
        activationId: record.activationId,
        customerId: record.customerId,
        planId: record.planId,
        eventType: 'ActivationCompleted',
        status: 'ACTIVE',
        details: { planId: record.planId },
        timestamp: record.updatedAt,
      });
      return;
    }

    await this.persistActivation(record);
  }

  getActivation(id: string): ActivationRecord | undefined {
    return this.activations.get(id);
  }

  getAllActivations(): ActivationRecord[] {
    return Array.from(this.activations.values());
  }
}
