package com.payflow.payflow_processor_service.service;

import com.payflow.payflow_processor_service.domain.OutboxEvent;
import com.payflow.payflow_processor_service.domain.OutboxStatus;
import com.payflow.payflow_processor_service.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPoller {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${payflow.kafka.topics.payment-completed}")
    private String paymentCompletedTopic;

    @Value("${payflow.kafka.topics.payment-failed}")
    private String paymentFailedTopic;

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void pollAndPublish() {
        List<OutboxEvent> pending =
                outboxEventRepository.findByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        if (pending.isEmpty()) return;

        log.info("Processor outbox poller: {} pending events", pending.size());

        for (OutboxEvent event : pending) {
            String topic = switch (event.getEventType()) {
                case "PAYMENT_COMPLETED" -> paymentCompletedTopic;
                case "PAYMENT_FAILED"    -> paymentFailedTopic;
                default -> throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
            };

            try {
                kafkaTemplate.send(topic, event.getAggregateId().toString(), event.getPayload()).get();
                outboxEventRepository.markAsPublished(event.getId(), Instant.now());
                log.info("Processor published: type={}, topic={}", event.getEventType(), topic);
            } catch (Exception e) {
                log.error("Processor failed to publish event {}: {}", event.getId(), e.getMessage());
                outboxEventRepository.markAsFailed(event.getId());
            }
        }
    }
}
