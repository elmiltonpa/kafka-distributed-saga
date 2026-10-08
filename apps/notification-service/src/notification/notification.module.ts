import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { NotificationController } from './notification.controller';
import { NotificationService } from './notification.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'notification-service-producer',
            brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
          },
          producerOnlyMode: true,
        },
      },
    ]),
    KafkaToolkitModule.register({
      dbName: 'notification_db',
    }),
  ],
  controllers: [NotificationController],
  providers: [NotificationService],
  exports: [NotificationService],
})
export class NotificationModule {}
