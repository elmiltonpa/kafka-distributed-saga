import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('NotificationBootstrap');

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: 'notification-service',
          brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
        },
        consumer: {
          groupId: 'notification-svc',
        },
      },
    },
  );

  await app.listen();
  logger.log(
    '🚀 notification-service conectado a Kafka (consumer group: notification-svc)',
  );
}

void bootstrap();
