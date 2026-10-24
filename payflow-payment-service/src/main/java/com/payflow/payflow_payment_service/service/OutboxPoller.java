package com.payflow.payflow_payment_service.service;

import com.payflow.payflow_payment_service.domain.OutboxEvent;
import com.payflow.payflow_payment_service.domain.OutboxStatus;
import com.payflow.payflow_payment_service.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * CONCEPT — Outbox Poller
 *
 * The core fix here: we use synchronous Kafka send (get()) instead of
 * async whenComplete(). This keeps everything on the Spring-managed
 * scheduling thread where @Transactional works correctly.
 *
 * Trade-off:
 *   Async (whenComplete): higher throughput, complex transaction management
 *   Sync (get):           simpler, works correctly, fine for low-medium volume
 *
 * For production high-throughput: use async + a dedicated @Transactional
 * service bean called from the callback (injected via ApplicationContext
 * to get the Spring proxy, not 'this').
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${payflow.kafka.topics.payment-initiated}")
    private String paymentInitiatedTopic;

    @Value("${payflow.kafka.topics.payment-completed}")
    private String paymentCompletedTopic;

    @Value("${payflow.kafka.topics.payment-failed}")
    private String paymentFailedTopic;

    @Scheduled(fixedDelayString = "${payflow.outbox.poll-interval-ms}")
    @Transactional
    public void pollAndPublish() {
        List<OutboxEvent> pendingEvents =
                outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Outbox poller found {} pending events", pendingEvents.size());

        for (OutboxEvent event : pendingEvents) {
            publishEvent(event);
        }
    }

    private void publishEvent(OutboxEvent event) {
        String topic = resolveTopic(event.getEventType());
        try {
            // Synchronous send — blocks until Kafka acknowledges.
            // Keeps us on the @Transactional scheduling thread so the
            // subsequent markAsPublished() is inside the same transaction.
            kafkaTemplate.send(topic, event.getAggregateId().toString(), event.getPayload()).get();

            outboxEventRepository.markAsPublished(event.getId(), Instant.now());
            log.info("Published outbox event id={}, type={}, topic={}",
                    event.getId(), event.getEventType(), topic);

        } catch (Exception e) {
            log.error("Failed to publish outbox event id={}, type={}: {}",
                    event.getId(), event.getEventType(), e.getMessage());
            outboxEventRepository.markAsFailed(event.getId());
        }
    }

    private String resolveTopic(String eventType) {
        return switch (eventType) {
            case "PAYMENT_INITIATED" -> paymentInitiatedTopic;
            case "PAYMENT_COMPLETED" -> paymentCompletedTopic;
            case "PAYMENT_FAILED"    -> paymentFailedTopic;
            default -> throw new IllegalArgumentException("Unknown event type: " + eventType);
        };
    }
}
