package com.payflow.payflow_payment_service.domain;

/**
 * CONCEPT — State Machine:
 * A payment moves through states in one direction only.
 * Valid transitions:
 *   PENDING → PROCESSING → COMPLETED
 *   PENDING → PROCESSING → FAILED
 *
 * You never go backwards (COMPLETED → PENDING is illegal).
 * The DB CHECK constraint enforces valid state names.
 * The service layer enforces valid transitions.
 *
 * In an interview: "How do you prevent a payment from being processed twice?"
 * Answer: The transition PENDING → PROCESSING is guarded by optimistic locking.
 * Only one thread can win the version-check UPDATE. The loser gets an
 * OptimisticLockException and must re-read the current state before retrying.
 */
public enum PaymentStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED
}
