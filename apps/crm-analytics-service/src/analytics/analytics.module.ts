import { Module } from '@nestjs/common';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { AnalyticsController } from './analytics.controller';
import { AnalyticsService } from './analytics.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'crm-analytics-producer',
            brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
          },
          producerOnlyMode: true,
        },
      },
    ]),
    KafkaToolkitModule.register({
      dbName: 'crm_analytics_db',
    }),
  ],
  controllers: [AnalyticsController],
  providers: [AnalyticsService],
  exports: [AnalyticsService],
})
export class AnalyticsModule {}
