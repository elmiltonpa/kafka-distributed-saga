import { EventEnvelope } from "../event-envelope";

export interface ProvisioningCompletedPayload {
  planId: string;
}

export type ProvisioningCompletedEvent =
  EventEnvelope<ProvisioningCompletedPayload>;

export interface ProvisioningFailedPayload {
  reason: string;
}

export type ProvisioningFailedEvent = EventEnvelope<ProvisioningFailedPayload>;
