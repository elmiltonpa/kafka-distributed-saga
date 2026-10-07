import { EventEnvelope } from "../event-envelope";

export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure?: "none" | "billing" | "provisioning";
}

export type ActivationRequestedEvent =
  EventEnvelope<ActivationRequestedPayload>;
