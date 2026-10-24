package com.payflow.payflow_notification_service.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payflow_notification_service.dto.NotificationMessage;
import com.payflow.payflow_notification_service.dto.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * CONCEPT — The Kafka-to-RabbitMQ Bridge:
 *
 * This class is the integration point between two messaging systems.
 * It:
 *   1. Consumes from Kafka (payment.completed, payment.failed)
 *   2. Transforms the Kafka event into a NotificationMessage
 *   3. Publishes to the RabbitMQ topic exchange with the right routing key
 *   4. RabbitMQ's exchange routes to the appropriate queues
 *
 * WHY not just listen to Kafka directly from the email/sms/webhook handlers?
 * Because Kafka and RabbitMQ have different delivery semantics:
 *
 * Kafka consumer group gets one copy per group.
 * If 3 handlers (email, sms, webhook) were all in the same Kafka group,
 * only ONE would receive each message — not all three.
 * You'd need 3 separate Kafka consumer groups to fan out, and this service
 * would need to manage 3 independent consumers.
 *
 * RabbitMQ with a topic exchange handles fan-out natively.
 * One Kafka consumer receives the message, publishes to RabbitMQ once,
 * and RabbitMQ fans it out to all bound queues automatically.
 * Clean separation of concerns.
 *
 * CONCEPT — Topics in one @KafkaListener:
 * You can listen to multiple topics in one annotation:
 *   topics = {"payment.completed", "payment.failed"}
 * Both go to the same method. We distinguish them by event.getEventType()
 * or by the ConsumerRecord's topic field.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentResultListener {

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;

    @Value("${payflow.rabbitmq.exchange}")
    private String exchange;

    @Value("${payflow.rabbitmq.routing-keys.completed}")
    private String completedRoutingKey;

    @Value("${payflow.rabbitmq.routing-keys.failed}")
    private String failedRoutingKey;

    @KafkaListener(
            topics = {
                "${payflow.kafka.topics.payment-completed}",
                "${payflow.kafka.topics.payment-failed}"
            },
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentResult(ConsumerRecord<String, String> record, Acknowledgment ack) {
        log.info("Received payment result from Kafka: topic={}, key={}",
                record.topic(), record.key());

        PaymentEvent event;
        try {
            String rawValue = record.value();
            if (rawValue.startsWith("\"") && rawValue.endsWith("\"")) {
                rawValue = objectMapper.readValue(rawValue, String.class);
            }
            event = objectMapper.readValue(rawValue, PaymentEvent.class);
        } catch (Exception e) {
            log.error("Failed to deserialise payment result event at offset {}: {}",
                    record.offset(), e.getMessage());
            ack.acknowledge(); // corrupt message — ack so we don't redeliver forever
            return;
        }

        // Transform Kafka event → notification message
        NotificationMessage notification = buildNotification(event);

        // Determine routing key based on event type
        // CONCEPT — Routing key determines which queues receive this message.
        // "payment.completed" → email.q + sms.q + webhook.q (per bindings)
        // "payment.failed"    → email.q only
        String routingKey = "PAYMENT_COMPLETED".equals(event.getEventType())
                ? completedRoutingKey
                : failedRoutingKey;

        try {
            // CONCEPT — RabbitTemplate.convertAndSend:
            // (exchange, routingKey, message object)
            // The Jackson2JsonMessageConverter (configured in RabbitMQConfig)
            // serialises the NotificationMessage to JSON automatically.
            rabbitTemplate.convertAndSend(exchange, routingKey, notification);

            log.info("Published notification to RabbitMQ: exchange={}, routingKey={}, paymentId={}",
                    exchange, routingKey, event.getPaymentId());

            // Only commit Kafka offset after successful RabbitMQ publish
            ack.acknowledge();

        } catch (Exception e) {
            // RabbitMQ publish failed — don't ack Kafka
            // Kafka will redeliver this message on the next poll
            log.error("Failed to publish to RabbitMQ for payment {}: {}",
                    event.getPaymentId(), e.getMessage());
            // Don't ack — Kafka redelivers, we retry next time
        }
    }

    private NotificationMessage buildNotification(PaymentEvent event) {
        return NotificationMessage.builder()
                .notificationId(UUID.randomUUID())
                .eventType(event.getEventType())
                .paymentId(event.getPaymentId())
                // Notify the sender (they initiated the payment)
                .recipientAccount(event.getSenderAccount())
                .amount(event.getAmount())
                .currency(event.getCurrency())
                .status(event.getStatus())
                .providerRef(event.getProviderRef())
                .failureReason(event.getFailureReason())
                .occurredAt(event.getOccurredAt())
                .build();
    }
}
