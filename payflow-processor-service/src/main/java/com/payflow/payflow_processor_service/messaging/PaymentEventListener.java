package com.payflow.payflow_processor_service.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payflow_processor_service.dto.PaymentEvent;
import com.payflow.payflow_processor_service.provider.SimulatedPaymentProvider;
import com.payflow.payflow_processor_service.service.PaymentProcessorService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * CONCEPT — The Kafka Listener and everything around it:
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  AT-LEAST-ONCE vs EXACTLY-ONCE DELIVERY                                │
 * │                                                                         │
 * │  Kafka guarantees AT-LEAST-ONCE delivery with manual offset commits.   │
 * │  This means: a message may be delivered MORE THAN ONCE.                │
 * │  It will NEVER be delivered zero times (unless the topic is purged).   │
 * │                                                                         │
 * │  WHY duplicates happen:                                                 │
 * │  1. Consumer processes message, crashes before committing offset        │
 * │  2. Kafka rebalances, reassigns partition to another consumer           │
 * │  3. New consumer re-reads from last committed offset → duplicate        │
 * │                                                                         │
 * │  EXACTLY-ONCE is possible in Kafka (v0.11+) with transactions, but     │
 * │  it's complex, has performance overhead, and requires all consumers     │
 * │  to participate. Most production systems choose:                        │
 * │                                                                         │
 * │    AT-LEAST-ONCE + IDEMPOTENT CONSUMER = effectively-once              │
 * │                                                                         │
 * │  The idempotency check (Redis) below makes this consumer safe to       │
 * │  receive duplicates without double-processing.                          │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  RETRY + EXPONENTIAL BACKOFF                                            │
 * │                                                                         │
 * │  When the provider call fails transiently, we retry with backoff:       │
 * │    Attempt 1: immediate                                                 │
 * │    Attempt 2: wait 1s                                                   │
 * │    Attempt 3: wait 2s                                                   │
 * │    Attempt 4: wait 4s → give up, send to DLQ                          │
 * │                                                                         │
 * │  WHY exponential backoff?                                               │
 * │  Linear retry (retry every 1s) hammers the provider when it's down.   │
 * │  Exponential backoff gives the provider time to recover. Combined      │
 * │  with jitter (randomised delay) it prevents the "thundering herd"      │
 * │  where 1000 consumers all retry at the exact same moment.             │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  DEAD LETTER QUEUE (DLQ)                                               │
 * │                                                                         │
 * │  After max retries, the message goes to a DLQ topic (payment.dlq).    │
 * │  The DLQ is for messages that:                                         │
 * │    - Failed permanently (bad data, invalid account)                    │
 * │    - Exhausted all retry attempts                                       │
 * │    - Need human intervention or a separate recovery process            │
 * │                                                                         │
 * │  Without a DLQ, a poison-pill message (one that always fails) blocks  │
 * │  the entire partition forever — no other messages in that partition    │
 * │  are processed until the bad one succeeds. DLQ sidelines it.          │
 * │                                                                         │
 * │  DLQ message includes original payload + failure metadata for          │
 * │  diagnosis and potential reprocessing.                                 │
 * └─────────────────────────────────────────────────────────────────────────┘
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventListener {

    private final PaymentProcessorService processorService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    @Value("${payflow.processor.max-retry-attempts:3}")
    private int maxRetryAttempts;

    @Value("${payflow.processor.retry-base-delay-ms:1000}")
    private long retryBaseDelayMs;

    @Value("${payflow.kafka.topics.payment-dlq}")
    private String dlqTopic;

    // CONCEPT — @KafkaListener:
    // groupId ties this listener to the consumer group defined in properties.
    // All instances of this service sharing the same groupId will collectively
    // consume the topic — Kafka divides partitions among them.
    //
    // containerFactory = "kafkaListenerContainerFactory" — defined in KafkaConfig.
    // That factory sets AckMode.MANUAL so we control when offsets are committed.
    @KafkaListener(
            topics = "${payflow.kafka.topics.payment-initiated}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentInitiated(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("Received Kafka message: topic={}, partition={}, offset={}, key={}",
                record.topic(), record.partition(), record.offset(), record.key());

        PaymentEvent event;
        try {
            // The Kafka message value may be a raw JSON object OR a JSON-encoded
            // string (double-serialized) depending on the producer's serializer config.
            // We handle both: if readValue fails with a String, unwrap it first.
            String rawValue = record.value();
            // Strip outer quotes if the value is a JSON string wrapping a JSON object
            if (rawValue.startsWith("\"") && rawValue.endsWith("\"")) {
                rawValue = objectMapper.readValue(rawValue, String.class);
            }
            event = objectMapper.readValue(rawValue, PaymentEvent.class);
        } catch (Exception e) {
            // Unparseable message — this is a poison pill. Send to DLQ immediately.
            // Retrying won't help — the payload is corrupt.
            log.error("Failed to deserialize Kafka message at offset {}: {}", record.offset(), e.getMessage());
            sendToDlq(record, "DESERIALIZATION_FAILED: " + e.getMessage());
            ack.acknowledge(); // commit offset so we don't redeliver this broken message
            return;
        }

        // ── Consumer-side idempotency check ───────────────────────────────────
        // CONCEPT: eventId is the unique identifier for this specific event.
        // We store it in Redis with a TTL. If we see it again (duplicate delivery),
        // we skip processing and just acknowledge the message.
        String processingKey = "processor:event:" + event.getEventId();
        Boolean alreadyProcessed = redisTemplate.opsForValue()
                .setIfAbsent(processingKey, "1", Duration.ofHours(24));

        if (Boolean.FALSE.equals(alreadyProcessed)) {
            log.info("Duplicate event {} detected — skipping (already processed)", event.getEventId());
            ack.acknowledge();
            return;
        }

        // ── Retry loop with exponential backoff ───────────────────────────────
        int attempt = 0;
        while (attempt <= maxRetryAttempts) {
            try {
                processorService.processPayment(event);
                // SUCCESS — commit the offset
                // CONCEPT: only commit AFTER successful processing.
                // If we crash here (after processing but before ack), Kafka redelivers.
                // The Redis idempotency key above handles that duplicate gracefully.
                ack.acknowledge();
                return;

            } catch (Exception e) {
                attempt++;
                log.warn("Payment processing attempt {}/{} failed for payment {}: {}",
                        attempt, maxRetryAttempts, event.getPaymentId(), e.getMessage());

                if (attempt > maxRetryAttempts) {
                    // Exhausted retries — send to DLQ
                    log.error("Max retries exceeded for payment {} — sending to DLQ", event.getPaymentId());
                    sendToDlq(record, "MAX_RETRIES_EXCEEDED after " + maxRetryAttempts + " attempts: " + e.getMessage());
                    ack.acknowledge(); // commit so we don't redeliver
                    // Clean up the Redis lock so a manual replay from DLQ can reprocess
                    redisTemplate.delete(processingKey);
                    return;
                }

                // CONCEPT — Exponential backoff with jitter:
                // delay = baseDelay * 2^(attempt-1) + random jitter
                // attempt 1: ~1s, attempt 2: ~2s, attempt 3: ~4s
                long delay = retryBaseDelayMs * (1L << (attempt - 1));
                long jitter = (long) (Math.random() * 200); // up to 200ms jitter
                sleepQuietly(delay + jitter);
            }
        }
    }

    private void sendToDlq(ConsumerRecord<String, String> originalRecord, String failureReason) {
        // CONCEPT — DLQ message design:
        // Include the original payload AND metadata about why it failed.
        // This lets an ops team inspect the DLQ and decide:
        //   - Fix the data and replay to the original topic
        //   - Discard as permanently invalid
        //   - Alert and manually process
        String dlqPayload = String.format(
                "{\"originalPayload\":%s,\"failureReason\":\"%s\",\"originalTopic\":\"%s\",\"originalOffset\":%d}",
                originalRecord.value(), failureReason, originalRecord.topic(), originalRecord.offset()
        );

        kafkaTemplate.send(dlqTopic, originalRecord.key(), dlqPayload);
        log.warn("Message sent to DLQ: key={}, reason={}", originalRecord.key(), failureReason);
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
