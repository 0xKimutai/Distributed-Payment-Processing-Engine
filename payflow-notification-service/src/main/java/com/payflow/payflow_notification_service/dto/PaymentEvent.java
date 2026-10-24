package com.payflow.payflow_notification_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * CONCEPT — Shared Event Contract:
 *
 * This is the same shape as PaymentEvent in the other two services.
 * In a real multi-repo setup you would publish this DTO as a shared Maven
 * artifact (e.g. payflow-events-api-1.0.0.jar) so all services import it.
 *
 * Advantages of a shared contract library:
 *   - Schema changes are caught at compile time across all consumers
 *   - One place to update the event shape
 *
 * Disadvantages:
 *   - Introduces a build dependency between otherwise independent services
 *   - A shared lib version bump forces all consumers to upgrade together
 *
 * Many teams prefer to duplicate the DTO (as we do here) and rely on
 * a Schema Registry for enforcement — keeping services truly independent.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private UUID eventId;
    private String eventType;      // PAYMENT_COMPLETED | PAYMENT_FAILED
    private Instant occurredAt;
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
