import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { ActivationsAggregatorController } from './activations-aggregator.controller';
import { ActivationsController } from './activations.controller';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsService } from './activations.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'activation-api-producer',
            brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [ActivationsController, ActivationsAggregatorController],
  providers: [ActivationsService, ActivationsGateway],
  exports: [ActivationsService, ActivationsGateway],
})
export class ActivationsModule {}
