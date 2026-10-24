-- =============================================================================
-- V1__create_payments_schema.sql
--
-- FLYWAY CONCEPT:
--   Flyway versions your DB schema just like Git versions your code.
--   Files must be named V{number}__{description}.sql
--   Once applied, NEVER edit this file — create V2__ instead.
--   Flyway tracks applied migrations in its own `flyway_schema_history` table.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto"; -- enables gen_random_uuid()

-- =============================================================================
-- TABLE: payments
--
-- IDEMPOTENCY CONCEPT:
--   The `idempotency_key` UNIQUE constraint is the core safety mechanism.
--   A client sends the same key on every retry of the same payment request.
--   If the key already exists, we return the existing payment — no double charge.
--   The DB enforces this even under race conditions (two threads hit INSERT
--   simultaneously — only one wins, the other gets a unique constraint violation).
--
-- OPTIMISTIC LOCKING CONCEPT:
--   The `version` column is used by JPA to prevent lost updates.
--   When you UPDATE a row, JPA adds: WHERE id = ? AND version = ?
--   If another thread already updated it (version changed), your update affects
--   0 rows → JPA throws OptimisticLockException → you retry or handle it.
--   This is better than pessimistic locking (SELECT FOR UPDATE) because it
--   doesn't hold locks and scales better under low-contention workloads.
-- =============================================================================
CREATE TABLE payments (
    id               UUID          NOT NULL DEFAULT gen_random_uuid(),
    idempotency_key  VARCHAR(255)  NOT NULL,
    amount           NUMERIC(19,4) NOT NULL,   -- NEVER use FLOAT for money
    currency         VARCHAR(3)    NOT NULL,
    sender_account   VARCHAR(255)  NOT NULL,
    receiver_account VARCHAR(255)  NOT NULL,
    status           VARCHAR(50)   NOT NULL DEFAULT 'PENDING',
    provider_ref     VARCHAR(255),              -- reference from payment provider
    failure_reason   TEXT,
    created_at       TIMESTAMP     NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP     NOT NULL DEFAULT now(),
    version          BIGINT        NOT NULL DEFAULT 0,  -- optimistic locking

    CONSTRAINT pk_payments PRIMARY KEY (id),
    CONSTRAINT uq_payments_idempotency_key UNIQUE (idempotency_key),
    CONSTRAINT chk_payments_status  CHECK (status IN ('PENDING','PROCESSING','COMPLETED','FAILED')),
    CONSTRAINT chk_payments_amount  CHECK (amount > 0),
    CONSTRAINT chk_payments_currency CHECK (currency ~ '^[A-Z]{3}$')
);

CREATE INDEX idx_payments_status     ON payments(status);
CREATE INDEX idx_payments_created_at ON payments(created_at DESC);

-- =============================================================================
-- TABLE: outbox_events
--
-- OUTBOX PATTERN CONCEPT:
--   Problem: saving a payment to Postgres AND publishing to Kafka are two
--   separate operations. If Kafka is down after the DB write, the event is lost.
--   There is no distributed transaction spanning Postgres + Kafka.
--
--   Solution — Transactional Outbox:
--   In ONE database transaction:
--     1. INSERT into payments
--     2. INSERT into outbox_events      ← same commit, atomically
--
--   A background poller (OutboxPoller.java) then:
--     3. SELECT * FROM outbox_events WHERE status = 'PENDING'
--     4. Publish each event to Kafka
--     5. UPDATE status = 'PUBLISHED'
--
--   If the poller crashes between step 4 and 5, it republishes on next run.
--   The Kafka consumer must therefore be idempotent (handles duplicates safely).
--   This is the "at-least-once delivery" guarantee.
-- =============================================================================
CREATE TABLE outbox_events (
    id           UUID         NOT NULL DEFAULT gen_random_uuid(),
    aggregate_id UUID         NOT NULL,   -- the payment ID this event belongs to
    event_type   VARCHAR(100) NOT NULL,   -- e.g. "PAYMENT_INITIATED"
    payload      JSONB        NOT NULL,   -- full event snapshot as JSON
    status       VARCHAR(50)  NOT NULL DEFAULT 'PENDING',
    created_at   TIMESTAMP    NOT NULL DEFAULT now(),
    published_at TIMESTAMP,

    CONSTRAINT pk_outbox_events PRIMARY KEY (id),
    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING','PUBLISHED','FAILED'))
);

-- PARTIAL INDEX CONCEPT:
--   The poller only queries WHERE status = 'PENDING'.
--   A partial index only indexes rows matching that condition.
--   Result: smaller index, faster queries, less I/O — exactly what you want
--   when 99% of rows are PUBLISHED and you only care about the small PENDING set.
CREATE INDEX idx_outbox_pending      ON outbox_events(created_at) WHERE status = 'PENDING';
CREATE INDEX idx_outbox_aggregate_id ON outbox_events(aggregate_id);
