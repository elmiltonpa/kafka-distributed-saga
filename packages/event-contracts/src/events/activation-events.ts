import { EventEnvelope } from "../event-envelope";

export interface ActivationCompletedPayload {
  planId: string;
}

export type ActivationCompletedEvent =
  EventEnvelope<ActivationCompletedPayload>;

export interface ActivationFailedPayload {
  reason: string;
}

export type ActivationFailedEvent = EventEnvelope<ActivationFailedPayload>;
