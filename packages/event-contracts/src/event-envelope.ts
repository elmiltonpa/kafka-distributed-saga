export interface EventEnvelope<TPayload = unknown> {
  eventId: string;
  eventType: string;
  version: number;
  occurredAt: string;
  correlationId: string;
  customerId: string;
  source: string;
  payload: TPayload;
}
