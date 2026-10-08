import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { Db, MongoClient, MongoClientOptions } from 'mongodb';

@Injectable()
export class MongoClientService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MongoClientService.name);
  private client: MongoClient | null = null;
  private db: Db | null = null;
  private isConnected = false;

  constructor(private readonly dbName: string) {}

  async onModuleInit(): Promise<void> {
    const uri = process.env.MONGO_URI || 'mongodb://localhost:27017';
    try {
      const options: MongoClientOptions = {
        serverSelectionTimeoutMS: 2000,
      } as any;
      this.client = new MongoClient(uri, options);
      await this.client.connect();
      this.db = this.client.db(this.dbName);
      this.isConnected = true;
      this.logger.log(`Conectado a MongoDB (db: ${this.dbName})`);
    } catch {
      this.isConnected = false;
      this.logger.warn(`MongoDB no disponible en ${uri}. Operando en memoria.`);
    }
  }

  async onModuleDestroy(): Promise<void> {
    if (this.client) {
      await this.client.close();
    }
  }

  getDb(): Db | null {
    return this.isConnected ? this.db : null;
  }

  isMongoAvailable(): boolean {
    return this.isConnected;
  }
}
