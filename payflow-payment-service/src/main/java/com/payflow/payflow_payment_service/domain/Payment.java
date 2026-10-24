package com.payflow.payflow_payment_service.domain;

import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * CONCEPT — JPA Entity with Optimistic Locking:
 *
 * @Version tells JPA to use the `version` column for optimistic locking.
 * When you call paymentRepository.save(payment), Hibernate generates:
 *   UPDATE payments SET status=?, version=2, ... WHERE id=? AND version=1
 *
 * If another transaction already incremented the version to 2, your WHERE
 * clause matches 0 rows → Hibernate throws OptimisticLockException.
 * You handle this by re-reading the entity and retrying.
 *
 * WHY this over SELECT FOR UPDATE (pessimistic locking)?
 *   - Optimistic: no locks held, scales well, best when conflicts are rare
 *   - Pessimistic: holds a lock until commit, serialises access, best when
 *     conflicts are frequent (e.g. a shared counter updated every millisecond)
 *   - For payments, conflicts are rare (same payment updated concurrently
 *     almost never happens in normal flow), so optimistic is the right choice.
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
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    // The client generates this key and sends it on every retry.
    // The UNIQUE constraint in the DB makes duplicate inserts fail safely.
    @Column(name = "idempotency_key", nullable = false, unique = true)
    private String idempotencyKey;

    // BigDecimal is the correct type for money in Java.
    // Never use double or float — they cannot represent 0.1 exactly in binary.
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
    @Builder.Default
    private PaymentStatus status = PaymentStatus.PENDING;

    // Set by the processor service after calling the payment provider
    @Column(name = "provider_ref")
    private String providerRef;

    @Column(name = "failure_reason", columnDefinition = "TEXT")
    private String failureReason;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    // OPTIMISTIC LOCKING — Hibernate manages this column automatically.
    // You never set it manually. Hibernate increments it on every update.
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }
}
