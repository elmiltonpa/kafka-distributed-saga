import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('ActivationApiBootstrap');
  const app = await NestFactory.create(AppModule);

  app.enableCors();

  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'activation-api-consumer',
        brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
      },
      consumer: {
        groupId: 'activation-api-aggregator',
      },
    },
  });

  await app.startAllMicroservices();
  logger.log(
    '📡 activation-api conectado a Kafka como Saga Aggregator (group: activation-api-aggregator)',
  );

  const port = process.env.PORT ?? 3000;
  await app.listen(port);
  logger.log(`🚀 activation-api HTTP escuchando en http://localhost:${port}`);
}

void bootstrap();
