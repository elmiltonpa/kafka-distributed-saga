import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import {
  TOPICS,
  type ActivationRequestedEvent,
  type EventEnvelope,
} from '@poc/event-contracts';
import { BillingService } from './billing.service';

@Controller()
export class BillingController {
  constructor(private readonly billingService: BillingService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() event: ActivationRequestedEvent) {
    await this.billingService.processActivationRequested(event);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() event: EventEnvelope<any>) {
    if (event.eventType === 'ActivationFailed') {
      await this.billingService.processActivationFailed(event);
    }
  }
}
