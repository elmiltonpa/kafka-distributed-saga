import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, type EventEnvelope } from '@poc/event-contracts';
import { ActivationsService } from './activations.service';

@Controller()
export class ActivationsAggregatorController {
  constructor(private readonly activationsService: ActivationsService) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  async handleBillingEvent(@Payload() event: EventEnvelope<any>) {
    await this.activationsService.handleBillingEvent(event);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  async handleProvisioningEvent(@Payload() event: EventEnvelope<any>) {
    await this.activationsService.handleProvisioningEvent(event);
  }
}
