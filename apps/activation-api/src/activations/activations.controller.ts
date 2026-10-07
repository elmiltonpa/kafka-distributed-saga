import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  NotFoundException,
  Param,
  Post,
} from '@nestjs/common';
import { ActivationsService } from './activations.service';
import { CreateActivationDto } from './dto/create-activation.dto';

@Controller('activations')
export class ActivationsController {
  constructor(private readonly activationsService: ActivationsService) {}

  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  async createActivation(@Body() dto: CreateActivationDto) {
    const activation = await this.activationsService.requestActivation(dto);

    return {
      activationId: activation.activationId,
      status: activation.status,
      customerId: activation.customerId,
      planId: activation.planId,
      simulateFailure: activation.simulateFailure,
      message: 'Activation request accepted and queued for processing',
      createdAt: activation.createdAt,
    };
  }

  @Get(':id')
  getActivationById(@Param('id') id: string) {
    const activation = this.activationsService.getActivation(id);
    if (!activation) {
      throw new NotFoundException(`Activacion con ID "${id}" no encontrada.`);
    }
    return activation;
  }

  @Get()
  getAllActivations() {
    return this.activationsService.getAllActivations();
  }
}
