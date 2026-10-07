import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { TOPICS, type ActivationRequestedEvent } from '@poc/event-contracts';
import { ProvisioningService } from './provisioning.service';

@Controller()
export class ProvisioningController {
  constructor(private readonly provisioningService: ProvisioningService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  async handleActivationRequested(@Payload() event: ActivationRequestedEvent) {
    await this.provisioningService.processActivationRequested(event);
  }
}
