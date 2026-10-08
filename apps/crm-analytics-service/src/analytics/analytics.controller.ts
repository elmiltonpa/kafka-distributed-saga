import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, type EventEnvelope } from '@poc/event-contracts';
import { AnalyticsService } from './analytics.service';

@Controller()
export class AnalyticsController {
  constructor(private readonly analyticsService: AnalyticsService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() event: EventEnvelope<unknown>) {
    await this.analyticsService.recordEvent(TOPICS.ACTIVATION_REQUESTED, event);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvents(@Payload() event: EventEnvelope<unknown>) {
    await this.analyticsService.recordEvent(TOPICS.BILLING_EVENTS, event);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvents(@Payload() event: EventEnvelope<unknown>) {
    await this.analyticsService.recordEvent(TOPICS.PROVISIONING_EVENTS, event);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() event: EventEnvelope<unknown>) {
    await this.analyticsService.recordEvent(TOPICS.ACTIVATION_EVENTS, event);
  }
}
