import { DynamicModule, Module } from '@nestjs/common';
import { MongoClientService } from './mongo/mongo-client.service';
import { IdempotencyService } from './idempotency/idempotency.service';
import { DlqService } from './resilience/dlq.service';

export interface KafkaToolkitOptions {
  dbName: string;
}

@Module({})
export class KafkaToolkitModule {
  static register(options: KafkaToolkitOptions): DynamicModule {
    const mongoProvider = {
      provide: MongoClientService,
      useFactory: async () => {
        const client = new MongoClientService(options.dbName);
        await client.onModuleInit();
        return client;
      },
    };

    const idempotencyProvider = {
      provide: IdempotencyService,
      useFactory: (mongo: MongoClientService) => new IdempotencyService(mongo),
      inject: [MongoClientService],
    };

    return {
      module: KafkaToolkitModule,
      providers: [mongoProvider, idempotencyProvider, DlqService],
      exports: [MongoClientService, IdempotencyService, DlqService],
    };
  }
}
