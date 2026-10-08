import { Inject, Injectable, Logger, Optional } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import * as nodemailer from 'nodemailer';
import { EventEnvelope, TOPICS } from '@poc/event-contracts';
import { DlqService, IdempotencyService, MongoClientService } from '@poc/kafka-toolkit';

export interface SentNotification {
  id: string;
  activationId: string;
  customerId: string;
  eventType: string;
  to: string;
  subject: string;
  sentAt: string;
}

@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);
  private readonly transporter: nodemailer.Transporter;
  private readonly sentNotifications: SentNotification[] = [];

  constructor(
    private readonly idempotencyService: IdempotencyService,
    private readonly dlqService: DlqService,
    private readonly mongoService: MongoClientService,
    @Optional() @Inject('KAFKA_CLIENT') private readonly kafkaClient?: ClientKafka,
  ) {
    const host = process.env.SMTP_HOST || 'localhost';
    const port = Number(process.env.SMTP_PORT) || 1025;

    this.transporter = nodemailer.createTransport({
      host,
      port,
      ignoreTLS: true,
    });

    this.logger.log(`Cliente SMTP configurado hacia ${host}:${port} (Mailhog)`);
  }

  private async persistNotification(notif: SentNotification) {
    this.sentNotifications.push(notif);
    const db = this.mongoService.getDb();
    if (db) {
      try {
        await db.collection('notifications').insertOne(notif);
      } catch (err) {
        this.logger.warn(`Error al persistir notificación en Mongo: ${err}`);
      }
    }
  }

  async processActivationEvent(event: EventEnvelope<any>) {
    await this.dlqService.executeWithRetry(TOPICS.ACTIVATION_EVENTS, event, async () => {
      const {
        eventId,
        eventType,
        correlationId,
        customerId,
        payload,
        occurredAt,
      } = event;
      const recipientEmail = `${customerId.toLowerCase()}@telecom-demo.com`;

      const alreadyProcessed = await this.idempotencyService.isAlreadyProcessed(eventId, 'notification-service');
      if (alreadyProcessed) {
        this.logger.warn(
          `🛡️ [Idempotencia] Notificación para evento [${eventId}] ya fue procesada. Descartando duplicado.`,
        );
        return;
      }

      this.logger.log(
        `[notification-service] Recibido [${eventType}] para cliente [${customerId}] (activationId=${correlationId})`,
      );

      if (eventType === 'ActivationCompleted') {
        const subject = `🎉 ¡Bienvenido! Tu plan ${payload.planId} ha sido activado con éxito`;
        const html = `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px;">
            <h2 style="color: #10b981;">¡Tu servicio ya está disponible!</h2>
            <p>Hola <strong>Cliente ${customerId}</strong>,</p>
            <p>Nos complace informarte que la activación de tu plan ha finalizado exitosamente a través de nuestra plataforma de eventos distribuida.</p>

            <table style="width: 100%; border-collapse: collapse; margin: 20px 0;">
              <tr style="background-color: #f9fafb;">
                <td style="padding: 10px; border: 1px solid #e5e7eb;"><strong>ID de Activación:</strong></td>
                <td style="padding: 10px; border: 1px solid #e5e7eb;">${correlationId}</td>
              </tr>
              <tr>
                <td style="padding: 10px; border: 1px solid #e5e7eb;"><strong>Plan contratado:</strong></td>
                <td style="padding: 10px; border: 1px solid #e5e7eb;">${payload.planId}</td>
              </tr>
              <tr style="background-color: #f9fafb;">
                <td style="padding: 10px; border: 1px solid #e5e7eb;"><strong>Fecha de activación:</strong></td>
                <td style="padding: 10px; border: 1px solid #e5e7eb;">${occurredAt}</td>
              </tr>
            </table>

            <p style="color: #6b7280; font-size: 13px;">Este correo es una simulación generada por la POC de Sistemas Distribuidos (UTN FRCU).</p>
          </div>
        `;

        await this.sendMail(
          recipientEmail,
          subject,
          html,
          correlationId,
          customerId,
          eventType,
        );
      } else if (eventType === 'ActivationFailed') {
        const subject = `⚠️ Aviso importante: No se pudo activar tu servicio (${payload.reason})`;
        const html = `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; border: 1px solid #e0e0e0; border-radius: 8px;">
            <h2 style="color: #ef4444;">No pudimos completar la activación de tu plan</h2>
            <p>Hola <strong>Cliente ${customerId}</strong>,</p>
            <p>Lamentamos informarte que la activación no pudo completarse debido al siguiente motivo:</p>

            <div style="background-color: #fee2e2; color: #991b1b; padding: 12px; border-radius: 6px; margin: 15px 0;">
              <strong>Motivo del fallo:</strong> ${payload.reason}
            </div>

            <p><strong>Compensación automática ejecutada:</strong> Si se había generado una cuenta de facturación preliminar, la misma fue dada de baja automáticamente sin cargos.</p>

            <p style="color: #6b7280; font-size: 13px;">ID de solicitud: ${correlationId} | Fecha: ${occurredAt}</p>
          </div>
        `;

        await this.sendMail(
          recipientEmail,
          subject,
          html,
          correlationId,
          customerId,
          eventType,
        );
      }
    }, this.kafkaClient);
  }

  private async sendMail(
    to: string,
    subject: string,
    html: string,
    activationId: string,
    customerId: string,
    eventType: string,
  ) {
    try {
      const info = await this.transporter.sendMail({
        from: '"Telecom Service Activation" <no-reply@telecom-poc.com>',
        to,
        subject,
        html,
      });

      this.logger.log(
        `✉️ Email despachado con éxito a Mailhog -> Para: ${to} | Asunto: "${subject}" | MessageId: ${info.messageId}`,
      );

      await this.persistNotification({
        id: info.messageId,
        activationId,
        customerId,
        eventType,
        to,
        subject,
        sentAt: new Date().toISOString(),
      });
    } catch (error) {
      this.logger.error(`Error al despachar email a Mailhog: ${String(error)}`);
      throw error;
    }
  }

  getSentNotifications() {
    return this.sentNotifications;
  }
}
