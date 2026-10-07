<!-- BEGIN:turborepo-agent-rules -->

# This is NOT the Turborepo you know

Turborepo configuration, task behavior, and CLI commands can vary between installed versions and may differ from your training data. Resolve the `turbo` package from this file's directory or relevant workspace; in monorepos, it may not be visible from the repository root. For example, run `node -p "require.resolve('turbo/package.json')"` from a workspace that depends on `turbo`.

Read `docs/README.md` inside that installed package first, then read the relevant pages from its `docs/` directory before changing Turborepo configuration or commands. Heed deprecation notices. These bundled docs match the installed package version and are available without network access.

This block is written and re-added by `turbo` before repository-scoped commands when an AI agent is detected. In the Turborepo source repository, its template is defined in `crates/turborepo-cli/src/cli/agent_guidance.rs`. Removing the managed block while updates are enabled means a later qualifying invocation will add it again. Set `"agentGuidance": false` in the root `turbo.json` or `turbo.jsonc` to opt out; this does not remove an existing block. Keep the block committed with your work to avoid an uncommitted change on the next agent invocation.
<!-- END:turborepo-agent-rules -->

---

# CONSIGNAS Y ESPECIFICACIÓN DEL PROYECTO (TP8)
**UTN FRCU - Ingeniería en Sistemas de Información - Sistemas Distribuidos (2026)**  
**Proyecto:** POC Activación de Servicios con Kafka (Documento de Arquitectura · Kafka, NestJS y React)

---

## 1. Resumen Ejecutivo
La Prueba de Concepto (POC) demuestra que la activación de un plan de telecomunicaciones/servicios puede resolverse **publicando un único evento en Apache Kafka**, sin que ningún servicio llame directamente a otro vía HTTP. Un cliente contrata un plan desde una UI web en React; eso dispara en paralelo facturación, aprovisionamiento, notificación y registro en CRM, todos coordinados por eventos a través de un patrón de **Saga coreografiada con agregador**.

---

## 2. Alcance del Proyecto

### En alcance (Incluido):
- **UI de demo**: Formulario de contratación de plan y timeline de eventos en vivo por WebSocket.
- **Cinco microservicios en NestJS**:
  1. `activation-api` (API REST, WebSocket Gateway y agregador de saga).
  2. `billing-service` (Facturación y compensación).
  3. `provisioning-service` (Aprovisionamiento con simulación de demoras y fallos).
  4. `notification-service` (Notificaciones por correo simulado).
  5. `crm-analytics-service` (Auditoría/historial pasivo de eventos).
- **Infraestructura**: Kafka con 1 broker en modo KRaft (sin ZooKeeper), Kafka UI (kafbat) y MongoDB.
- **Resiliencia y patrones distribuidos**:
  - Simulación de fallos forzados desde la UI.
  - Mecanismos de compensación ante fallos.
  - Idempotencia en cada consumidor.
  - Reintentos exponenciales y Dead Letter Queue (DLQ).
- **Guión de demostración**: Ejecución de 5 escenarios para validar propiedades de Kafka.

### Fuera de alcance:
- Integración con BSS, pasarelas de pago o proveedores de telecomunicaciones reales.
- Autenticación y autorización de usuarios.
- Alta disponibilidad en producción (múltiples brokers de Kafka, réplicas, despliegue en la nube).
- Pruebas de carga y métricas de rendimiento masivas.
- Schema Registry y trazas distribuidas completas (fase opcional).

---

## 3. Stack Tecnológico

| Componente | Tecnología | Justificación / Rol |
| :--- | :--- | :--- |
| **Mensajería** | Apache Kafka (modo KRaft, 1 broker) | Sin ZooKeeper; suficiente para demostrar particiones, offsets y consumer groups. |
| **Servicios** | NestJS + `@nestjs/microservices` (transporte Kafka) | Uso de `@EventPattern` y `ClientKafka`; estructura tipada, modular y desacoplada. |
| **Frontend** | React + Vite + `socket.io-client` | Arranque veloz y comunicación WebSocket en tiempo real para el timeline. |
| **Persistencia** | MongoDB | Documentos flexibles para el estado de activación; transacciones sobre replica set de 1 nodo. |
| **Observabilidad** | Kafka UI (`kafbat/kafka-ui`) | Vista en vivo de tópicos, particiones, mensajes y lag de consumidores. |
| **Email simulado** | Mailhog | Servidor SMTP local y bandeja web para verificar notificaciones emitidas. |
| **Entorno local** | Docker Compose | Orquestación completa de la infraestructura con un único comando (`docker compose up`). |
| **Monorepo** | pnpm workspaces + Turborepo | Contratos y librerías compartidas entre servicios frontend y backend. |

---

## 4. Arquitectura del Sistema

`activation-api` es el único punto de entrada HTTP público; el resto del sistema reacciona exclusivamente a eventos en Kafka.

### Componentes:
1. **Demo UI (React + Vite)**:
   - Formulario "Contratar plan" (`customerId`, `planId`, selector de simulación de fallo: `none`, `billing`, `provisioning`).
   - Timeline de eventos en tiempo real recibidos por WebSocket.
2. **`activation-api` (NestJS)**:
   - Expone `POST /activations` (responde `202 Accepted` inmediato con estado `PENDING`) y `GET /activations/:id`.
   - Publica `ActivationRequested` en Kafka.
   - Escucha los resultados de `billing` y `provisioning`, mantiene el estado de la saga y publica `ActivationCompleted` o `ActivationFailed`.
   - Gateway `@WebSocketGateway` para emitir cada actualización a la UI en vivo.
3. **`billing-service` (NestJS)**:
   - Consume `ActivationRequested` y crea la cuenta de facturación simulada.
   - Consume `ActivationFailed` para compensar (anular la cuenta creada).
4. **`provisioning-service` (NestJS)**:
   - Consume `ActivationRequested` y simula aprovisionamiento con retardo de 1 a 3 segundos. Puede fallar a pedido según el flag de simulación.
5. **`notification-service` (NestJS)**:
   - Consume `activation.events` (`ActivationCompleted` o `ActivationFailed`) y envía email de bienvenida o alerta a Mailhog.
6. **`crm-analytics-service` (NestJS)**:
   - Consume todos los tópicos y audita los eventos sin emitir nada. Demuestra desacoplamiento total al sumar consumidores sin modificar a los productores.
7. **MongoDB**:
   - Una base de datos lógica por servicio. Colecciones comunes para `outbox` y `processed_events`.
8. **Kafka UI**:
   - Panel de control para visualizar tópicos, particiones, offsets y consumer groups.

---

## 5. Requerimientos del Sistema

### Requerimientos Funcionales (RF)
- **RF-01**: La UI permite crear una activación indicando `customerId`, `planId` y un modo de fallo opcional (`none`, `billing`, `provisioning`).
- **RF-02**: `POST /activations` responde `202 Accepted` con el `activationId` y estado `PENDING`, sin esperar a los demás servicios.
- **RF-03**: `activation-api` publica `ActivationRequested` por cada activación creada.
- **RF-04**: `billing` y `provisioning` procesan el mismo evento en paralelo y publican su resultado (éxito o fallo).
- **RF-05**: Con `billing` y `provisioning` en estado OK, la activación pasa a `ACTIVE` y se publica `ActivationCompleted`.
- **RF-06**: Si alguno falla, la activación pasa a `FAILED`, se publica `ActivationFailed` y `billing` anula la cuenta si la había creado (compensación).
- **RF-07**: `notification-service` envía un email por cada `ActivationCompleted` o `ActivationFailed`.
- **RF-08**: La UI muestra cada evento de la activación en una timeline, en tiempo real, vía WebSocket.
- **RF-09**: `GET /activations/:id` devuelve el estado actual y el historial completo de eventos.
- **RF-10**: `crm-analytics` registra todos los eventos y puede reprocesar el historial desde el inicio.

### Requerimientos No Funcionales (RNF)
- **RNF-01**: Todo el entorno se levanta con `docker compose up`.
- **RNF-02**: Los eventos de un mismo cliente se procesan en estricto orden (clave de partición en Kafka: `customerId`).
- **RNF-03**: Cada consumidor es idempotente (un evento repetido no produce efectos duplicados).
- **RNF-04**: Un mensaje que falla 3 veces va a la DLQ y no bloquea la partición.
- **RNF-05**: Si un servicio se cae y vuelve a iniciar, procesa los eventos pendientes sin perder ninguno.
- **RNF-06**: Todos los eventos llevan `correlationId` para rastrear una activación de punta a punta.
- **RNF-07**: Los contratos de eventos residen en una librería compartida y cuentan con versionado.

---

## 6. Tópicos y Contratos de Eventos

Cuatro tópicos de negocio más sus respectivas DLQ. Todos configurados con **3 particiones**, retención de **7 días** y particionamiento por clave **`customerId`**:

| Tópico | Eventos Publicados | Productor | Consumidores (Consumer Groups) |
| :--- | :--- | :--- | :--- |
| `activation.requested` | `ActivationRequested` | `activation-api` | `billing-svc`, `provisioning-svc`, `crm-analytics` |
| `billing.events` | `BillingAccountCreated`, `BillingFailed`, `BillingAccountCancelled` | `billing-service` | `activation-api`, `crm-analytics` |
| `provisioning.events` | `ProvisioningCompleted`, `ProvisioningFailed` | `provisioning-service` | `activation-api`, `crm-analytics` |
| `activation.events` | `ActivationCompleted`, `ActivationFailed` | `activation-api` | `notification-svc`, `billing-svc`, `crm-analytics` |
| `<topic>.dlq` | Mensajes que fallaron tras 3 reintentos | El consumidor que falló | Inspección manual en Kafka UI |

### Sobre Común de Eventos (Event Envelope)
```json
{
  "eventId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "eventType": "ActivationRequested",
  "version": 1,
  "occurredAt": "2026-09-27T16:40:00Z",
  "correlationId": "act-0001",
  "customerId": "C-1234",
  "source": "activation-api",
  "payload": {
    "planId": "FLOW-FULL",
    "channel": "web",
    "simulateFailure": "none"
  }
}
```

### Campos del Sobre:
- `eventId`: UUID único del evento; base de la idempotencia.
- `eventType` + `version`: Permite evolucionar el contrato sin romper consumidores existentes.
- `correlationId`: Identificador de la activación (`activationId`); agrupa los eventos de una solicitud.
- `customerId`: Clave de partición en Kafka; garantiza orden estricto de eventos por cliente.
- `source`: Nombre del servicio que publicó el evento.
- `payload`: Datos específicos del evento.

### Convenciones:
- Nombres de eventos en tiempo pasado en inglés (describen hechos consumados).
- Nombres de tópicos en formato `dominio.tipo` (ej: `billing.events`).

---

## 7. Flujo de la Saga y Máquina de Estados

La saga se gestiona como **coreografía con agregador**:
- `billing` y `provisioning` reaccionan en paralelo a `ActivationRequested`.
- `activation-api` agrega las respuestas y define el estado final.

### Transición de Estados en `activation-api`:
| Estado | Cuándo se alcanza | Estado Siguiente |
| :--- | :--- | :--- |
| **`PENDING`** | Al crearse la solicitud vía REST | `IN_PROGRESS` |
| **`IN_PROGRESS`** | Al recibir el primer resultado de billing o provisioning | `ACTIVE` o `FAILED` |
| **`ACTIVE`** | Cuando ambos servicios (billing y provisioning) confirmaron éxito | Final |
| **`FAILED`** | Cuando cualquiera de los dos falló | Final (dispara compensación) |

> **Caso borde a considerar:** Si provisioning falla antes de que billing cree la cuenta, billing puede recibir `ActivationFailed` antes de terminar su proceso. Billing debe manejar este escenario anulando la cuenta incluso si la creación culmina tras haberse emitido el fallo.

---

## 8. Modelo de Persistencia (MongoDB)

Cada servicio posee su propia base de datos (desacople de almacenamiento):

| Colección | Servicio | Contenido | Índices Clave |
| :--- | :--- | :--- | :--- |
| `activations` | `activation-api` | Estado de activación, resultados parciales e historial | `_id = activationId`, `customerId` |
| `billing_accounts` | `billing-service` | Cuenta simulada con estado `CREATED` o `CANCELLED` | `activationId` (único) |
| `provisioning_orders` | `provisioning-service` | Orden de provisión simulada | `activationId` (único) |
| `notifications` | `notification-service` | Registro de correos emitidos | `activationId + eventType` |
| `event_log` | `crm-analytics-service` | Copia de cada evento recibido | `eventId` (único), `correlationId` |
| `outbox` | Común a productores | Eventos pendientes de publicación | `status + createdAt` |
| `processed_events` | Común a consumidores | Registro de `eventId` para idempotencia | `eventId` (único), TTL de 7 días |

---

## 9. Decisiones Técnicas Clave

1. **Idempotencia en Consumidores**:
   - Cada consumidor registra el `eventId` en `processed_events` dentro de una transacción. Si el id ya existe (error de clave duplicada), se descarta el mensaje y se comitea el offset de Kafka.
2. **Reintentos y DLQ**:
   - Ante errores técnicos, se reintenta 3 veces con espera creciente (1 s, 2 s, 4 s). Si persiste, se redirige el mensaje a `<topic>.dlq` con el motivo en headers y se continúa con el siguiente mensaje.
   - Los errores de negocio simulados (`simulateFailure`) no van a la DLQ: se emiten como eventos regulares de fallo (`BillingFailed`, `ProvisioningFailed`).
3. **Commit Manual de Offsets**:
   - El commit de offset se realiza únicamente después de haber procesado y persistido con éxito el mensaje.
4. **MongoDB Replica Set**:
   - Configurado en replica set de 1 nodo para habilitar el uso de sesiones y transacciones ACID.
5. **Outbox Transaccional (Fase 4 / Opcional)**:
   - Para evitar pérdida de eventos en caídas entre el guardado en base y la publicación en Kafka.

---

## 10. Guión de Demostración (5 Escenarios)

La presentación se ejecuta con pantalla dividida (Demo UI a la izquierda, Kafka UI a la derecha):

| # | Escenario | Acción | Resultado Visible | Concepto de Kafka |
| :-: | :--- | :--- | :--- | :--- |
| **1** | **Camino feliz** | Contratar un plan sin fallos (`simulateFailure: 'none'`). | Timeline con 4 eventos en verde; email de bienvenida recibido en Mailhog. | **Fan-out**: Un evento genera múltiples reacciones independientes. |
| **2** | **Fallo y compensación** | Contratar forzando fallo en aprovisionamiento (`simulateFailure: 'provisioning'`). | Eventos `ProvisioningFailed` y `ActivationFailed`; facturación cancela la cuenta (`BillingAccountCancelled`); email de error. | **Saga sin llamadas directas** y compensación por eventos. |
| **3** | **Servicio caído** | 1. `docker compose stop notification-service`<br>2. Crear 3 contrataciones.<br>3. `docker compose start notification-service` | Kafka UI muestra lag de 3 mensajes; al reiniciar el servicio, los 3 emails se procesan y despachan. | **Gestión de Offsets**: Ningún mensaje se pierde aunque el servicio esté caído. |
| **4** | **Escalabilidad** | `docker compose up -d --scale billing-service=2` | Kafka UI muestra las 3 particiones repartidas dinámicamente entre las 2 instancias. | **Consumer Groups y Particionado**. |
| **5** | **Nuevo consumidor** | Levantar nuevo servicio loyalty con lectura desde el inicio (`auto.offset.reset=earliest`). | Procesa todo el histórico sin afectar a los productores ni a los otros servicios. | **Replay de Logs / Event Sourcing**. |

---

## 11. Plan de Implementación por Fases

- **Fase 1 · Base**:
  - Infraestructura en Docker Compose (Kafka KRaft, Kafka UI, Mongo, Mailhog).
  - `activation-api`, `billing-service` y `provisioning-service`.
  - Camino feliz comprobable por logs y Kafka UI (RF-01 a RF-05).
- **Fase 2 · Visible**:
  - Frontend con timeline WebSocket y Mailhog visible.
  - Simulación de fallos y flujo de compensación (Escenarios 1 y 2).
- **Fase 3 · Robusta**:
  - Idempotencia en consumidores, reintentos y DLQ.
  - `crm-analytics-service` y servicio `loyalty` (Escenarios 3 a 5).
- **Fase 4 · Opcional / Piloto**:
  - Outbox transaccional, Schema Registry y OpenTelemetry.

---

## 12. Criterios de Aceptación
- [ ] `docker compose up` levanta toda la arquitectura sin pasos manuales.
- [ ] Los 5 escenarios de demostración se ejecutan de punta a punta en menos de 15 minutos.
- [ ] Reenviar un evento previamente procesado no duplica registros ni emails (idempotencia comprobada).
- [ ] Un mensaje inválido termina en la DLQ correspondiente y la partición continúa consumiendo.
- [ ] Todo el flujo de una activación puede auditarse e identificarse mediante su `correlationId`.
