import { Injectable, Logger } from '@nestjs/common';
import { Collection } from 'mongodb';
import { MongoClientService } from '../mongo/mongo-client.service';

export interface ProcessedEventDocument {
  eventId: string;
  source: string;
  createdAt: Date;
}

@Injectable()
export class IdempotencyService {
  private readonly logger = new Logger(IdempotencyService.name);
  private readonly memoryStore = new Set<string>();
  private indexCreated = false;

  constructor(private readonly mongoService: MongoClientService) {}

  private getCollection(): Collection<ProcessedEventDocument> | null {
    const db = this.mongoService.getDb();
    return db ? db.collection<ProcessedEventDocument>('processed_events') : null;
  }

  private async ensureIndex(collection: Collection<ProcessedEventDocument>): Promise<void> {
    if (this.indexCreated) return;
    try {
      await collection.createIndex({ eventId: 1 }, { unique: true });
      await collection.createIndex({ createdAt: 1 }, { expireAfterSeconds: 604800 });
      this.indexCreated = true;
    } catch {
      this.indexCreated = true;
    }
  }

  async isAlreadyProcessed(eventId: string, source: string): Promise<boolean> {
    const collection = this.getCollection();
    if (!collection) {
      if (this.memoryStore.has(eventId)) {
        return true;
      }
      this.memoryStore.add(eventId);
      return false;
    }

    await this.ensureIndex(collection);

    try {
      await collection.insertOne({
        eventId,
        source,
        createdAt: new Date(),
      });
      return false;
    } catch (err: any) {
      if (err?.code === 11000) {
        return true;
      }
      if (this.memoryStore.has(eventId)) {
        return true;
      }
      this.memoryStore.add(eventId);
      return false;
    }
  }
}
