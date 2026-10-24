package com.payflow.payflow_processor_service.repository;

import com.payflow.payflow_processor_service.domain.OutboxEvent;
import com.payflow.payflow_processor_service.domain.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    List<OutboxEvent> findByStatusOrderByCreatedAtAsc(OutboxStatus status);

    @Modifying
    @Query("UPDATE OutboxEvent o SET o.status = 'PUBLISHED', o.publishedAt = :publishedAt WHERE o.id = :id")
    void markAsPublished(UUID id, Instant publishedAt);

    @Modifying
    @Query("UPDATE OutboxEvent o SET o.status = 'FAILED' WHERE o.id = :id")
    void markAsFailed(UUID id);
}
