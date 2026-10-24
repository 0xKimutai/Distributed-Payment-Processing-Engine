package com.payflow.payflow_processor_service.domain;

import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The processor service reads AND updates the payments table.
 * It transitions status: PENDING → PROCESSING → COMPLETED | FAILED
 *
 * CONCEPT — Shared database in microservices:
 * Sharing a DB between services is the "shared database" integration pattern.
 * It's a pragmatic choice here because:
 *   - The processor needs to update payment status atomically with writing the outbox event
 *   - A separate DB would require a distributed transaction (2PC) or saga pattern
 *
 * The tradeoff: tight coupling at the data layer.
 * In a strict microservices architecture, the processor would call the payment
 * service's internal API to update status. We keep it simple here since both
 * services are in the same bounded context (payment processing).
 */
@Entity
@Table(name = "payments")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Payment {

    @Id
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    @Column(name = "amount", nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(name = "currency", nullable = false, length = 3)
    private String currency;

    @Column(name = "sender_account", nullable = false)
    private String senderAccount;

    @Column(name = "receiver_account", nullable = false)
    private String receiverAccount;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private PaymentStatus status;

    @Column(name = "provider_ref")
    private String providerRef;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    // OPTIMISTIC LOCKING — same version column as in payment-service.
    // When the processor updates status, Hibernate checks the version.
    // If the payment-service or another processor instance already updated it,
    // this throws OptimisticLockException → we re-read and handle gracefully.
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }
}
