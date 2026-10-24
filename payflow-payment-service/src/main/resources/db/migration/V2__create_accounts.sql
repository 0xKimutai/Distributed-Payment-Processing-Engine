-- =============================================================================
-- V2__create_accounts.sql
--
-- Simulated accounts table.
-- In a real system accounts would live in a separate Account Service.
-- Here we keep it simple — one DB, one schema, accounts as reference data.
-- The payment service validates that sender has sufficient balance before
-- creating a payment.
-- =============================================================================

CREATE TABLE accounts (
    id             UUID          NOT NULL DEFAULT gen_random_uuid(),
    account_number VARCHAR(20)   NOT NULL,
    owner_name     VARCHAR(255)  NOT NULL,
    phone_number   VARCHAR(15)   NOT NULL,
    balance        NUMERIC(19,4) NOT NULL DEFAULT 0,
    currency       VARCHAR(3)    NOT NULL DEFAULT 'KES',
    status         VARCHAR(20)   NOT NULL DEFAULT 'ACTIVE',
    created_at     TIMESTAMP     NOT NULL DEFAULT now(),

    CONSTRAINT pk_accounts PRIMARY KEY (id),
    CONSTRAINT uq_accounts_number UNIQUE (account_number),
    CONSTRAINT uq_accounts_phone  UNIQUE (phone_number),
    CONSTRAINT chk_accounts_balance CHECK (balance >= 0),
    CONSTRAINT chk_accounts_status CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED'))
);

CREATE INDEX idx_accounts_number ON accounts(account_number);
