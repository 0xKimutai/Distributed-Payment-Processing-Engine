package com.payflow.payflow_payment_service.domain;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;
import java.util.UUID;

/**
 * CONCEPT — Outbox Pattern entity:
 *
 * This table is the "outbox" in the Transactional Outbox Pattern.
 * It acts as a staging area for events that need to be published to Kafka.
 *
 * The key insight: writing to this table happens in the SAME transaction
 * as writing to the payments table. So the event record is created
 * atomically with the payment record — they either both exist or neither does.
 *
 * The OutboxPoller picks up PENDING rows and publishes them to Kafka.
 * Only after a successful Kafka publish does the row get marked PUBLISHED.
 *
 * PAYLOAD as String (JSONB in DB):
 * We store the full event payload as a JSON string. This is a snapshot
 * of the data at the time the event was created. This is important because
 * by the time the poller runs, the payment may have been updated — we want
 * to publish the event with the data it had when it was created, not the
 * current state.
 */
@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    // Which payment this event relates to
    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    // e.g. "PAYMENT_INITIATED", "PAYMENT_COMPLETED", "PAYMENT_FAILED"
    @Column(name = "event_type", nullable = false)
    private String eventType;

    // The full event data as JSON — stored as JSONB in Postgres
    // PostgreSQL requires an explicit cast for jsonb columns.
    // Using columnDefinition = "jsonb" tells Hibernate the DB type,
    // but we also need @JdbcTypeCode to handle the Java↔DB type mapping correctly.
    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private OutboxStatus status = OutboxStatus.PENDING;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
    }
}
