# Event-Driven Service Activation Engine

A distributed, event-driven microservices proof of concept demonstrating an **asynchronous choreographed Saga pattern** with Kafka, NestJS, and MongoDB.

This project showcases how to eliminate synchronous HTTP bottlenecks between services during high-volume lifecycle operations (e.g., service activations, checkout, provisioning) by coordinating state through distributed log events, idempotent consumers, automated compensating transactions, and dead-letter queues (DLQ).

---

## Architecture Highlights & Backend Patterns

- **Choreographed Saga with State Aggregator**: Eliminates direct service-to-service HTTP dependencies. Services act autonomously upon receiving domain events, while `activation-api` aggregates parallel task completions to finalize or abort the saga.
- **Compensating Transactions (Rollbacks)**: If any step fails (e.g., technical provisioning error), an `ActivationFailed` event triggers automatic rollbacks across preceding services (e.g., cancelling the billing account).
- **Strict Partition Key Ordering**: Events are partitioned by `customerId` across Kafka partitions, guaranteeing strict FIFO order per customer while scaling horizontally across consumer instances.
- **Idempotent Consumers**: Every consumer verifies event uniqueness against transactional storage (`processed_events`) before execution, ensuring safe message replay and preventing duplicate side-effects.
- **Fault Tolerance & Dead Letter Queues (DLQ)**: Configured with exponential retry intervals. Persistent technical failures are diverted to dedicated `<topic>.dlq` topics without blocking partition progress.
- **Event Sourcing & Stream Replay**: Decoupled event logs allow new downstream consumers (e.g., `loyalty-service`) to replay historical event logs (`auto.offset.reset: earliest`) without impacting production producers.
- **Monorepo Architecture (Turborepo + pnpm)**: Centralized TypeScript repository featuring strictly typed contracts (`@poc/event-contracts`) and shared Kafka reliability decorators/helpers (`@poc/kafka-toolkit`).

---

### Event Envelope Format

All events published to Kafka follow a structured, versioned schema:

```json
{
  "eventId": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "eventType": "ActivationRequested",
  "version": 1,
  "occurredAt": "2026-10-08T02:00:00.000Z",
  "correlationId": "act-9842",
  "customerId": "C-1042",
  "source": "activation-api",
  "payload": {
    "planId": "FIBER-500MB",
    "channel": "web",
    "simulateFailure": "none"
  }
}
```

---

## Services & Modules

### Applications (`apps/`)

| Service | Technology | Role & Key Responsibilities |
| :--- | :--- | :--- |
| **`activation-api`** | NestJS, Socket.io | Public HTTP ingestion gateway (`POST /activations`), WebSocket event emitter, and Saga state aggregator. |
| **`billing-service`** | NestJS, KafkaJS | Creates customer billing accounts and listens for `ActivationFailed` to trigger compensating cancellations. |
| **`provisioning-service`** | NestJS, KafkaJS | Simulates hardware/network provisioning with configurable latencies and fault injection. |
| **`notification-service`** | NestJS, Nodemailer | Consumes lifecycle termination events and dispatches asynchronous emails to Mailhog. |
| **`crm-analytics-service`** | NestJS, KafkaJS | Passive event audit logger demonstrating total producer-consumer decoupling. |
| **`loyalty-service`** | NestJS, KafkaJS | Demonstrates late-joining consumer stream replay from partition offset `0`. |
| **`web`** | React 19, Vite, Tailwind | Interactive dashboard featuring failure injection toggles and real-time WebSocket event timeline. |

### Shared Packages (`packages/`)

- **`@poc/event-contracts`**: Shared TypeScript interfaces, topic constants, event schemas, and enum definitions.
- **`@poc/kafka-toolkit`**: Reusable Kafka utilities, retry logic, DLQ routing, consumer idempotency middleware, and event builders.

---

## Infrastructure Stack

- **Apache Kafka (KRaft mode)**: Single-broker setup operating without ZooKeeper. 3 partitions per topic with retention policies.
- **Kafka UI (`kafbat/kafka-ui`)**: Web dashboard for real-time partition, consumer group lag, and topic inspection.
- **MongoDB (Single-node Replica Set)**: Provides multi-document ACID transactions across service-isolated collections and idempotency tables.
- **Mailhog**: Local SMTP server with browser-based inbox for verifying dispatched notifications.

---

## Getting Started

### Prerequisites

- **Node.js** (v20+ recommended)
- **pnpm** (v9+)
- **Docker** & **Docker Compose**

### 1. Start Infrastructure Containers

```bash
docker compose up -d
```

Verify containers are healthy via `docker compose ps` (Kafka, MongoDB, Mailhog, and Kafka UI).

### 2. Install Dependencies & Build Workspace

```bash
pnpm install
pnpm build
```

### 3. Run Microservices & Web Client

```bash
pnpm dev
```

Turborepo will concurrently start all backend microservices along with the frontend client.

---

## Endpoints & Dashboards

| Component | URL | Description |
| :--- | :--- | :--- |
| **Demo Web UI** | [http://localhost:5173](http://localhost:5173) | Frontend control panel & real-time WebSocket timeline |
| **Activation API** | [http://localhost:3000](http://localhost:3000) | REST API & WebSocket server |
| **Kafka UI** | [http://localhost:8080](http://localhost:8080) | Topic partitions, consumer offsets, and message viewer |
| **Mailhog Web Inbox** | [http://localhost:8025](http://localhost:8025) | Simulated transactional email inspector |

---

## Key Scenarios to Verify

1. **Happy Path (Fan-out & Parallel Processing)**
   - Submit an activation with `simulateFailure: 'none'`.
   - Observe parallel processing across `billing` and `provisioning`, successful saga completion, and email notification delivery.
2. **Saga Failure & Automatic Compensation**
   - Submit an activation with `simulateFailure: 'provisioning'`.
   - `provisioning-service` produces `ProvisioningFailed`.
   - `activation-api` aggregates failure and issues `ActivationFailed`.
   - `billing-service` receives the failure and issues a compensating `BillingAccountCancelled` event.
3. **Consumer Lag & Broker Buffering (Resilience)**
   - Stop a consumer: `docker compose stop notification-service` (or stop its local process).
   - Trigger multiple activations. Check consumer lag accumulation in Kafka UI.
   - Restart the consumer: all pending events are consumed without loss or duplication.
4. **Partition Balancing & Scale-out**
   - Scale billing workers: `docker compose up -d --scale billing-service=2`.
   - Observe rebalancing of the 3 topic partitions across the consumer group in Kafka UI.
