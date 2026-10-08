import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, type EventEnvelope } from '@poc/event-contracts';
import { NotificationService } from './notification.service';

@Controller()
export class NotificationController {
  constructor(private readonly notificationService: NotificationService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  async handleActivationEvent(@Payload() event: EventEnvelope<any>) {
    await this.notificationService.processActivationEvent(event);
  }
}
