import { EventEnvelope } from "../event-envelope";

export interface BillingAccountCreatedPayload {
  billingAccountId: string;
}

export type BillingAccountCreatedEvent =
  EventEnvelope<BillingAccountCreatedPayload>;

export interface BillingFailedPayload {
  reason: string;
}

export type BillingFailedEvent = EventEnvelope<BillingFailedPayload>;

export interface BillingAccountCancelledPayload {
  billingAccountId: string;
}

export type BillingAccountCancelledEvent =
  EventEnvelope<BillingAccountCancelledPayload>;
