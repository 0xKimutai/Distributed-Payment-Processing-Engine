package com.payflow.payflow_payment_service.repository;

import com.payflow.payflow_payment_service.domain.OutboxEvent;
import com.payflow.payflow_payment_service.domain.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // The poller calls this to find events to publish.
    // Matches the partial index: idx_outbox_pending WHERE status = 'PENDING'
    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(OutboxStatus status);

    // Bulk update — more efficient than loading entities and saving one by one
    // when marking a batch of events as published
    @Modifying
    @Query("UPDATE OutboxEvent o SET o.status = 'PUBLISHED', o.publishedAt = :publishedAt WHERE o.id = :id")
    void markAsPublished(UUID id, Instant publishedAt);

    @Modifying
    @Query("UPDATE OutboxEvent o SET o.status = 'FAILED' WHERE o.id = :id")
    void markAsFailed(UUID id);
}
