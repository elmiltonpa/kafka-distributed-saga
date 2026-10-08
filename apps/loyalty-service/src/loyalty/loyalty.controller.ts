import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, type EventEnvelope } from '@poc/event-contracts';
import { LoyaltyService } from './loyalty.service';

@Controller()
export class LoyaltyController {
  constructor(private readonly loyaltyService: LoyaltyService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvents(@Payload() event: EventEnvelope<any>) {
    await this.loyaltyService.processActivationEvent(event);
  }
}
