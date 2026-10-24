package com.payflow.payflow_payment_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * CONCEPT — Event Schema Design:
 *
 * An event is an immutable fact: "something happened at a point in time."
 * It should contain enough data for consumers to act without needing to
 * call back to the payment service (avoid chatty microservices).
 *
 * EVENT ENVELOPE PATTERN:
 * Every event has metadata (eventId, eventType, occurredAt) + a payload.
 * The eventId is used by consumers for idempotency — if the same event
 * arrives twice (at-least-once delivery), the consumer can check:
 * "have I already processed eventId X?" and skip it if so.
 *
 * This is the consumer-side of the idempotency story.
 * The producer side is the idempotency key on the payment request.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {

    // Unique ID for THIS event — used by consumers for deduplication
    private UUID eventId;

    // The type drives consumer routing logic
    private String eventType;  // "PAYMENT_INITIATED" | "PAYMENT_COMPLETED" | "PAYMENT_FAILED"

    // When the event occurred — not when it was published (could be later)
    private Instant occurredAt;

    // The payment data — a snapshot at the time of the event
    private UUID paymentId;
    private String idempotencyKey;
    private BigDecimal amount;
    private String currency;
    private String senderAccount;
    private String receiverAccount;
    private String status;
    private String providerRef;
    private String failureReason;
}
