# PayFlow — Distributed Payment Processing Engine

A production-grade distributed payment processing backend built to demonstrate
event-driven architecture, fault tolerance, and distributed systems patterns at scale.

The system accepts payment requests, records them safely with full idempotency
guarantees, processes them asynchronously through a Kafka-driven pipeline, and fans
out notifications to downstream channels via RabbitMQ — without double-charging,
losing work, or breaking under partial failures.

---

## Architecture

```
CLIENT
  │  POST /api/v1/payments  { idempotencyKey, amount, currency, ... }
  ▼
┌──────────────────────────────────────────────────────────────────┐
│  payflow-payment-service  :8080                                   │
│                                                                  │
│  1. Redis idempotency check  → cache hit? return immediately     │
│  2. DB idempotency check     → row exists? return it             │
│  3. @Transactional:                                              │
│       INSERT payments (status=PENDING)                           │
│       INSERT outbox_events (type=PAYMENT_INITIATED)  ← same TX  │
│  4. Return 202 Accepted                                          │
│                                                                  │
│  OutboxPoller (every 5s):                                        │
│    SELECT PENDING → kafkaTemplate.send → mark PUBLISHED          │
└─────────────────────────┬────────────────────────────────────────┘
                          │  Kafka: payment.initiated
                          │  key = paymentId  (ordering per payment)
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  payflow-processor-service  :8083                                 │
│                                                                  │
│  KafkaListener (group=payflow-processor-group, 3 threads):       │
│  1. Deserialise → corrupt message? → DLQ immediately            │
│  2. Redis SET NX on eventId → duplicate? → skip                  │
│  3. UPDATE payments WHERE status='PENDING' → compare-and-swap   │
│  4. Call SimulatedPaymentProvider (200–800ms, 20% fail rate)     │
│  5. @Transactional:                                              │
│       UPDATE payments (status=COMPLETED|FAILED)                  │
│       INSERT outbox_events (PAYMENT_COMPLETED|PAYMENT_FAILED)    │
│  6. ack.acknowledge() — offset committed only after success      │
│                                                                  │
│  Transient failure: retry with exponential backoff (1s→2s→4s)   │
│  Permanent failure / max retries exhausted: → payment.dlq       │
│  OutboxPoller: publish COMPLETED/FAILED events to Kafka          │
└────────────┬──────────────────────────┬──────────────────────────┘
             │ payment.completed         │ payment.failed
             ▼                           ▼
┌──────────────────────────────────────────────────────────────────┐
│  payflow-notification-service  :8084                              │
│                                                                  │
│  KafkaListener (group=payflow-notification-group):               │
│  → transform PaymentEvent → NotificationMessage                  │
│  → rabbitTemplate.convertAndSend(exchange, routingKey, msg)      │
│                                                                  │
│  RabbitMQ topic exchange: payment.results                        │
│    payment.completed → email.notifications                       │
│                      → sms.notifications                         │
│                      → webhook.notifications                     │
│    payment.failed    → email.notifications  (only)               │
└──────────────────────────────────────────────────────────────────┘
```

---

## Services

| Service | Port | Role |
|---------|------|------|
| `payflow-payment-service` | `8080` | REST API — accepts and records payments |
| `payflow-processor-service` | `8083` | Kafka consumer — calls payment provider, updates status |
| `payflow-notification-service` | `8084` | Kafka consumer — routes results to RabbitMQ channels |

---

## Infrastructure

| Component | Port | Purpose |
|-----------|------|---------|
| PostgreSQL 16 | `5440` | Payments table + outbox_events table |
| Redis 7 | `6382` | Idempotency cache (producer + consumer side) |
| Kafka (Confluent 7.6) | `9092` | Primary event bus |
| Zookeeper | `2181` | Kafka cluster coordination |
| RabbitMQ 3.13 | `5672` / `15672` | Notification fan-out (Management UI on 15672) |

> **Note:** Non-standard host ports (`5440`, `6382`) are used to avoid conflicts with local Postgres and Redis installations.

---

## Kafka Topics

| Topic | Producer | Consumer(s) | Partitions |
|-------|----------|-------------|------------|
| `payment.initiated` | payment-service (via outbox poller) | processor-service | 3 |
| `payment.completed` | processor-service (via outbox poller) | notification-service | 3 |
| `payment.failed` | processor-service (via outbox poller) | notification-service | 3 |
| `payment.dlq` | processor-service | ops / monitoring | 3 |

All messages are keyed by `paymentId`. This guarantees all events for a given payment
land on the same partition and are processed in order.

---

## RabbitMQ Topology

```
payment.results  (topic exchange)
      │
      ├── binding: payment.completed  →  email.notifications
      ├── binding: payment.completed  →  sms.notifications
      ├── binding: payment.completed  →  webhook.notifications
      └── binding: payment.failed     →  email.notifications
```

Each queue has a dead-letter exchange configured. Nacked messages route to
`email.notifications.dlq`, `sms.notifications.dlq`, `webhook.notifications.dlq`.

---

## Database Schema

```sql
-- Flyway migrations in payflow-payment-service/src/main/resources/db/migration/

-- V1: payments + outbox_events
payments (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  idempotency_key  VARCHAR(255) UNIQUE NOT NULL,  -- double-charge prevention
  amount           NUMERIC(19,4) NOT NULL,         -- never FLOAT for money
  currency         VARCHAR(3) NOT NULL,
  sender_account   VARCHAR(255) NOT NULL,
  receiver_account VARCHAR(255) NOT NULL,
  status           VARCHAR(50) NOT NULL,           -- PENDING|PROCESSING|COMPLETED|FAILED
  provider_ref     VARCHAR(255),
  failure_reason   TEXT,
  version          BIGINT NOT NULL DEFAULT 0,      -- optimistic locking
  created_at, updated_at TIMESTAMP NOT NULL
)

outbox_events (
  id           UUID PRIMARY KEY,
  aggregate_id UUID NOT NULL,           -- paymentId this event belongs to
  event_type   VARCHAR(100) NOT NULL,   -- PAYMENT_INITIATED|COMPLETED|FAILED
  payload      JSONB NOT NULL,          -- full event snapshot at creation time
  status       VARCHAR(50) NOT NULL,    -- PENDING|PUBLISHED|FAILED
  created_at, published_at TIMESTAMP
)

-- V2: accounts (simulated — in production this would be a separate service)
-- V3: 20 seeded accounts with varying balances for testing
```

---

## Key Design Patterns

### Idempotency (two layers)

Every `POST /payments` request carries a client-generated `idempotencyKey`.

1. **Redis (Layer 1):** sub-millisecond cache check — absorbs all retries without touching the DB
2. **DB UNIQUE constraint (Layer 2):** safety net for cache misses and race conditions — only one INSERT can win

```bash
# Same key sent 4 times → same payment ID every time, 1 row in DB
curl -X POST http://localhost:8080/api/v1/payments \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "your-unique-key-here",
    "amount": "5000.00",
    "currency": "KES",
    "senderAccount": "ACC-001",
    "receiverAccount": "ACC-016"
  }'
```

### Transactional Outbox Pattern

Saving a payment to Postgres and publishing to Kafka are two separate systems with
no shared transaction. The outbox pattern solves this:

- Payment INSERT and outbox_events INSERT happen in **one database transaction**
- A `@Scheduled` poller (every 5 seconds) relays PENDING outbox rows to Kafka
- If Kafka is unavailable, events wait safely in Postgres until it recovers
- The poller uses synchronous `kafkaTemplate.send().get()` so the DB update
  (`status=PUBLISHED`) is in the same `@Transactional` context as the send

### At-Least-Once + Idempotent Consumer

Kafka guarantees at-least-once delivery. The processor defends against duplicates
at two levels:

- **Redis `SET NX` on `eventId`:** atomic first-seen check — duplicate events are silently skipped
- **Compare-and-swap on payment status:** `UPDATE payments WHERE status='PENDING'` — only one
  consumer instance can transition a payment, concurrent duplicates get 0 rows updated and exit cleanly

### Retry with Exponential Backoff

Transient provider failures (timeouts, 503s) are retried:

```
Attempt 1: immediate
Attempt 2: wait 1s  + jitter
Attempt 3: wait 2s  + jitter
Attempt 4: wait 4s  + jitter → give up, send to DLQ
```

Jitter prevents thundering herd when many consumers retry simultaneously.
Permanent failures (`INSUFFICIENT_FUNDS`, `INVALID_ACCOUNT`) skip retries entirely
and go straight to FAILED status.

### Manual Kafka Offset Commit

```java
// AckMode.MANUAL — offset committed only after successful processing
public void onPaymentInitiated(ConsumerRecord<String, String> record, Acknowledgment ack) {
    processPayment(event);  // process first
    ack.acknowledge();      // commit offset only on success
    // crash here → Kafka redelivers → idempotency check handles it
}
```

### Optimistic Locking

The `version` column on the `payments` table prevents lost updates under concurrency:

```sql
UPDATE payments SET status='PROCESSING', version=2 WHERE id=? AND version=1
```

Zero rows updated means another thread won — the loser backs off cleanly without
holding any locks.

---

## Prerequisites

- Java 17+
- Maven (or use the included `mvnw` wrapper)
- Docker + Docker Compose

---

## Running Locally

### 1. Start infrastructure

```bash
docker compose up -d
```

Wait for all containers to be healthy:

```bash
docker compose ps
```

Expected output: all five containers (`payflow-postgres`, `payflow-redis`,
`payflow-kafka`, `payflow-zookeeper`, `payflow-rabbitmq`) showing `healthy`.

> **If Kafka shows `Exited (1)`** — this is a known Zookeeper stale-node issue after
> container restarts. Fix it with:
> ```bash
> docker compose restart zookeeper && sleep 8 && docker compose restart kafka
> ```

### 2. Start the services (in separate terminals)

```bash
# Terminal 1 — Payment API
cd payflow-payment-service && ./mvnw spring-boot:run

# Terminal 2 — Payment Processor
cd payflow-processor-service && ./mvnw spring-boot:run

# Terminal 3 — Notification Service
cd payflow-notification-service && ./mvnw spring-boot:run
```

Flyway runs automatically on the payment service's first start and creates all
tables. The processor and notification services validate the schema but do not run
migrations.

### 3. Verify all services are healthy

```bash
curl http://localhost:8080/actuator/health   # payment-service
curl http://localhost:8083/actuator/health   # processor-service
curl http://localhost:8084/actuator/health   # notification-service
```

All should return `{"status": "UP"}`.

---

## API Reference

### POST /api/v1/payments

Initiates a new payment. Idempotent — safe to retry any number of times with the same `idempotencyKey`.

**Request:**
```json
{
  "idempotencyKey": "a-unique-string-at-least-10-chars",
  "amount": "5000.00",
  "currency": "KES",
  "senderAccount": "ACC-001",
  "receiverAccount": "ACC-016"
}
```

**Response `202 Accepted`:**
```json
{
  "id": "6831a9a2-1746-4f72-b47e-fd6ce9ec6e77",
  "idempotencyKey": "a-unique-string-at-least-10-chars",
  "amount": 5000.00,
  "currency": "KES",
  "senderAccount": "ACC-001",
  "receiverAccount": "ACC-016",
  "status": "PENDING",
  "providerRef": null,
  "failureReason": null,
  "createdAt": "2026-10-24T07:00:00Z",
  "updatedAt": "2026-10-24T07:00:00Z"
}
```

Returns `200 OK` (not `202`) if the payment already existed (idempotency hit).

### GET /api/v1/payments/{id}

Polls the current status of a payment. Use this after receiving `202 Accepted`.

**Response `200 OK`:**
```json
{
  "id": "6831a9a2-1746-4f72-b47e-fd6ce9ec6e77",
  "status": "COMPLETED",
  "providerRef": "SIM-1792824663004-EC9EB808",
  "failureReason": null
}
```

### Payment Status Lifecycle

```
PENDING → PROCESSING → COMPLETED
                     → FAILED
```

### Error Responses (RFC 7807 Problem Details)

```json
{
  "type": "https://payflow.com/errors/validation",
  "title": "Validation Failed",
  "status": 400,
  "detail": "idempotencyKey: must be between 10 and 255 characters"
}
```

| Status | Type | Cause |
|--------|------|-------|
| `400` | `validation` | Invalid request fields |
| `404` | `not-found` | Payment ID does not exist |
| `409` | `duplicate-payment` | Idempotency key reused with different parameters |
| `500` | `internal` | Unexpected server error |

---

## Seeded Test Accounts

The database is pre-seeded with 20 accounts across three balance tiers:

| Accounts | Balance | Scenario |
|----------|---------|---------|
| `ACC-001` to `ACC-005` | KES 95,000 – 410,000 | High-balance senders |
| `ACC-006` to `ACC-013` | KES 500 – 75,000 | Medium / low balance |
| `ACC-014`, `ACC-015` | KES 0 | Zero balance — all payments fail |
| `ACC-016` to `ACC-020` | — | Receiver accounts |

Use any `ACC-001` to `ACC-013` as sender and any `ACC-016` to `ACC-020` as receiver.

---

## Observing the System

### Watch the full pipeline

```bash
# 1. Send a payment
curl -s -X POST http://localhost:8080/api/v1/payments \
  -H "Content-Type: application/json" \
  -d '{
    "idempotencyKey": "observe-this-payment-001",
    "amount": "1500.00",
    "currency": "KES",
    "senderAccount": "ACC-001",
    "receiverAccount": "ACC-016"
  }'

# 2. Watch the outbox publish to Kafka (within 5s)
psql postgresql://payflow:payflow_secret@localhost:5440/payflow_db \
  -c "SELECT event_type, status, published_at FROM outbox_events ORDER BY created_at DESC LIMIT 5;"

# 3. Poll for final status
curl http://localhost:8080/api/v1/payments/{id}

# 4. Check all payment statuses
psql postgresql://payflow:payflow_secret@localhost:5440/payflow_db \
  -c "SELECT idempotency_key, status, provider_ref, failure_reason FROM payments ORDER BY created_at DESC;"
```

### RabbitMQ Management UI

Open [http://localhost:15672](http://localhost:15672) — credentials `payflow` / `payflow_secret`.

Navigate to **Queues** to see `email.notifications`, `sms.notifications`, and
`webhook.notifications` with message counts and delivery rates in real time.

### Kafka consumer group lag

```bash
docker exec payflow-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group payflow-processor-group \
  --describe
```

`LAG=0` on all partitions means every event has been consumed and acknowledged.

### Inspect the DLQ

```bash
docker exec payflow-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic payment.dlq \
  --from-beginning \
  --timeout-ms 3000
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 17 |
| Framework | Spring Boot 4.1 |
| REST API | Spring MVC |
| Database ORM | Spring Data JPA / Hibernate |
| Schema migrations | Flyway |
| Primary database | PostgreSQL 16 |
| Cache | Redis 7 |
| Event streaming | Apache Kafka (Confluent 7.6) |
| Message broker | RabbitMQ 3.13 (AMQP) |
| Containerisation | Docker / Docker Compose |
| Build tool | Maven |
| Utilities | Lombok |

---

## Project Structure

```
payflow/
├── docker-compose.yml                  # full local infrastructure
├── payflow-payment-service/            # REST API + outbox writer
│   └── src/main/
│       ├── java/.../
│       │   ├── config/                 # Kafka topics, Redis, Jackson
│       │   ├── controller/             # PaymentController (POST, GET)
│       │   ├── domain/                 # Payment, OutboxEvent, enums
│       │   ├── dto/                    # PaymentRequest, PaymentResponse, PaymentEvent
│       │   ├── exception/              # GlobalExceptionHandler (RFC 7807)
│       │   ├── repository/             # PaymentRepository, OutboxEventRepository
│       │   └── service/                # PaymentService, OutboxPoller
│       └── resources/
│           ├── application.properties
│           └── db/migration/           # V1 schema, V2 accounts, V3 seed data
│
├── payflow-processor-service/          # Kafka consumer + payment provider
│   └── src/main/java/.../
│       ├── config/                     # KafkaConfig (AckMode.MANUAL, concurrency=3)
│       ├── domain/                     # Payment, OutboxEvent (shared DB)
│       ├── dto/                        # PaymentEvent
│       ├── messaging/                  # PaymentEventListener (retry, DLQ)
│       ├── provider/                   # SimulatedPaymentProvider
│       ├── repository/
│       └── service/                    # PaymentProcessorService, OutboxPoller
│
└── payflow-notification-service/       # Kafka → RabbitMQ bridge
    └── src/main/java/.../
        ├── config/                     # RabbitMQConfig (exchange, queues, bindings, DLX)
        ├── dto/                        # PaymentEvent, NotificationMessage
        ├── messaging/                  # PaymentResultListener
        └── service/                    # EmailNotificationHandler
                                        # SmsNotificationHandler
                                        # WebhookNotificationHandler
```

---

## Design Decisions and Tradeoffs

**Why Flyway over `ddl-auto`?**
Schema changes are code. They go through PR review, are versioned, and reproduce
identically across every environment. `ddl-auto=update` silently drops columns in
some scenarios — never acceptable in a payment system.

**Why Kafka for the payment pipeline but RabbitMQ for notifications?**
Kafka's durable, replayable log is the right model for payment events — you need
ordering per payment, multiple independent consumer groups, and the ability to replay
from any offset. RabbitMQ's topic exchange model is a better fit for notification
routing — a single Kafka event fans out to email, SMS, and webhook queues via binding
rules, with zero changes to the producer when new channels are added.

**Why optimistic locking over `SELECT FOR UPDATE`?**
Pessimistic locking holds a row lock until the transaction commits, serialising all
concurrent updates. For payments, the same record being updated simultaneously is rare
in normal flow. Optimistic locking holds no locks, scales better, and the loser simply
retries after detecting the conflict.

**Why synchronous Kafka send in the outbox poller?**
`kafkaTemplate.send().get()` blocks until Kafka acknowledges. This keeps the send
and the subsequent `markAsPublished()` DB update in the same `@Transactional` context.
Using async `whenComplete()` callbacks breaks this guarantee because the callback fires
on a Kafka I/O thread outside Spring's transaction management.

**Why `NUMERIC(19,4)` and `BigDecimal` for amounts?**
Binary floating point (`double`, `float`) cannot represent 0.1 exactly. At millions
of transactions, rounding errors accumulate. `NUMERIC` is exact in SQL; `BigDecimal`
is exact in Java. There is no valid reason to use floating point for monetary values.
