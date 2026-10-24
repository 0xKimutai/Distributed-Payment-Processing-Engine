package com.payflow.payflow_notification_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * CONCEPT — Message Transformation (Anti-Corruption Layer):
 *
 * PaymentEvent (from Kafka) is a raw domain event — everything about the payment.
 * NotificationMessage is a shaped payload for notification channels.
 *
 * Downstream handlers (email, SMS, webhook) don't need to know about Kafka
 * event internals. If the upstream schema changes, only this mapping updates.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationMessage {
    private UUID notificationId;
    private String eventType;          // PAYMENT_COMPLETED | PAYMENT_FAILED
    private UUID paymentId;
    private String recipientAccount;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String providerRef;
    private String failureReason;
    private Instant occurredAt;
}
