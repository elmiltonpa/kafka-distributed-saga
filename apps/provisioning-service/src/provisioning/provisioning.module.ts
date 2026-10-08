import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { ProvisioningController } from './provisioning.controller';
import { ProvisioningService } from './provisioning.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'provisioning-service-producer',
            brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
          },
          producerOnlyMode: true,
        },
      },
    ]),
    KafkaToolkitModule.register({
      dbName: 'provisioning_db',
    }),
  ],
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
  exports: [ProvisioningService],
})
export class ProvisioningModule {}
