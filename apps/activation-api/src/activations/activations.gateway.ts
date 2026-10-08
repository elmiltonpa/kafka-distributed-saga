import { Logger } from '@nestjs/common';
import {
  OnGatewayConnection,
  OnGatewayDisconnect,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';

@WebSocketGateway({
  cors: {
    origin: '*',
  },
})
export class ActivationsGateway
  implements OnGatewayConnection, OnGatewayDisconnect
{
  @WebSocketServer()
  server!: Server;

  private readonly logger = new Logger(ActivationsGateway.name);

  handleConnection(client: Socket) {
    this.logger.log(`Cliente WebSocket conectado: ${client.id}`);
  }

  handleDisconnect(client: Socket) {
    this.logger.log(`Cliente WebSocket desconectado: ${client.id}`);
  }

  emitActivationUpdate(payload: {
    activationId: string;
    customerId: string;
    planId: string;
    eventType: string;
    status: string;
    details?: any;
    timestamp: string;
  }) {
    this.logger.log(
      `[WebSocket] Emitiendo 'activationUpdate' [${payload.activationId}] -> ${payload.eventType} (${payload.status})`,
    );
    this.server?.emit('activationUpdate', payload);
  }
}
