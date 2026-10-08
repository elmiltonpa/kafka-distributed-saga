import { Injectable, Logger } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { EventEnvelope } from '@poc/event-contracts';

export interface DlqPayload<T = unknown> {
  originalTopic: string;
  originalEvent: EventEnvelope<T>;
  error: {
    message: string;
    stack?: string;
  };
  retryCount: number;
  failedAt: string;
}

@Injectable()
export class DlqService {
  private readonly logger = new Logger(DlqService.name);

  async sendToDlq<T>(
    originalTopic: string,
    event: EventEnvelope<T>,
    error: Error | any,
    retryCount: number,
    kafkaClient?: ClientKafka,
  ): Promise<void> {
    const dlqTopic = `${originalTopic}.dlq`;
    const messagePayload: DlqPayload<T> = {
      originalTopic,
      originalEvent: event,
      error: {
        message: error?.message || String(error),
        stack: error?.stack,
      },
      retryCount,
      failedAt: new Date().toISOString(),
    };

    this.logger.error(
      `[DLQ] Mensaje desviado a ${dlqTopic} tras ${retryCount} reintentos fallidos. Motivo: ${messagePayload.error.message}`,
    );

    if (kafkaClient) {
      try {
        kafkaClient.emit(dlqTopic, {
          key: event.customerId || event.correlationId,
          value: messagePayload,
        });
      } catch (err) {
        this.logger.error(`[DLQ] Error al emitir mensaje a ${dlqTopic}:`, err);
      }
    }
  }

  async executeWithRetry<T>(
    topic: string,
    event: EventEnvelope<T>,
    handler: () => Promise<void>,
    kafkaClient?: ClientKafka,
    maxRetries = 3,
  ): Promise<void> {
    let attempt = 0;
    const delays = [1000, 2000, 4000];

    while (attempt <= maxRetries) {
      try {
        await handler();
        return;
      } catch (error: any) {
        attempt++;
        if (attempt > maxRetries) {
          await this.sendToDlq(topic, event, error, maxRetries, kafkaClient);
          return;
        }
        const delay = delays[attempt - 1] || 4000;
        this.logger.warn(
          `[Reintento ${attempt}/${maxRetries}] Fallo en tópico ${topic}. Esperando ${delay}ms... Causa: ${error?.message || error}`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
}
