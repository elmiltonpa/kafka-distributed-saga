import { Injectable, Logger } from '@nestjs/common';
import { EventEnvelope } from '@poc/event-contracts';

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

  private readonly processedEvents = new Set<string>();

  recordEvent(topic: string, event: EventEnvelope<unknown>) {
    const {
      eventId,
      eventType,
      source,
      customerId,
      correlationId,
      occurredAt,
      payload,
    } = event;

    if (this.processedEvents.has(eventId)) {
      this.logger.warn(
        `🛡️ [Idempotencia] Evento auditado previamente [${eventId}] (${eventType}) descartado.`,
      );
      return;
    }
    this.processedEvents.add(eventId);

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

    this.eventLog.push(auditedEvent);

    this.logger.log(
      `📊 [CRM Analytics Audit] Tópico: "${topic}" | Evento: [${eventType}] | Cliente: [${customerId}] | ActivationId: [${correlationId}] | Fuente: [${source}] (Total auditados: ${this.eventLog.length})`,
    );
  }

  getAuditLog(): AuditedEvent[] {
    return this.eventLog;
  }

  getEventsByActivation(correlationId: string): AuditedEvent[] {
    return this.eventLog.filter((e) => e.correlationId === correlationId);
  }
}
