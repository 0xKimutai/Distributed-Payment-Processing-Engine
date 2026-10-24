package com.payflow.payflow_processor_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * CONCEPT — Event Contract:
 * This DTO is the schema of the Kafka message produced by the payment-service.
 * In production you'd enforce this with a Schema Registry (Avro or Protobuf)
 * so producers and consumers can't accidentally break each other's schema.
 *
 * Without a Schema Registry, a producer changing a field name silently breaks
 * all consumers — they get null values with no error. This is called a
 * "silent schema drift" and it's a common failure mode in event-driven systems.
 *
 * Schema Registry solves this by rejecting incompatible schema changes at publish time.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {
    private UUID eventId;
    private String eventType;
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
