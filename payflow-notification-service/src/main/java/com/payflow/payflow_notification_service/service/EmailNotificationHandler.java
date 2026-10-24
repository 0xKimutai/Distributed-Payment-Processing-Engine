package com.payflow.payflow_notification_service.service;

import com.payflow.payflow_notification_service.dto.NotificationMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * CONCEPT — RabbitMQ @RabbitListener:
 *
 * Each handler listens on its own queue. RabbitMQ delivered the message
 * to this queue based on the routing key matching the binding we declared
 * in RabbitMQConfig. This handler doesn't know about the exchange or routing
 * rules — it just consumes its queue.
 *
 * CONCEPT — Competing Consumers:
 * If you run 3 instances of this service, all 3 listen on email.notifications.
 * RabbitMQ delivers each message to ONE of the 3 instances (round-robin or
 * based on prefetch). This is horizontal scaling for queue processing.
 *
 * Compare to Kafka: with 3 partitions and 3 consumer instances in the same
 * group, each instance owns one partition. With RabbitMQ, all instances
 * share the same queue — messages are distributed dynamically.
 *
 * CONCEPT — Acknowledgment in RabbitMQ:
 * basicAck  → message processed successfully, delete from queue
 * basicNack → message failed:
 *   requeue=true  → put back in queue, retry immediately
 *   requeue=false → send to DLX (dead letter exchange) → DLQ
 *
 * Spring AMQP handles ack/nack automatically based on whether the method
 * completes normally or throws an exception.
 */
@Component
@Slf4j
public class EmailNotificationHandler {

    // Receives both payment.completed and payment.failed (per binding in RabbitMQConfig)
    @RabbitListener(queues = "${payflow.rabbitmq.queues.email}")
    public void handleEmailNotification(NotificationMessage message) {
        log.info("[EMAIL] Sending notification for payment={}, status={}, recipient={}",
                message.getPaymentId(), message.getStatus(), message.getRecipientAccount());

        // Simulate sending email
        if ("PAYMENT_COMPLETED".equals(message.getEventType())) {
            log.info("[EMAIL] ✅ Payment of {} {} to account {} was successful. Ref: {}",
                    message.getAmount(), message.getCurrency(),
                    message.getRecipientAccount(), message.getProviderRef());
        } else {
            log.info("[EMAIL] ❌ Payment of {} {} to account {} failed. Reason: {}",
                    message.getAmount(), message.getCurrency(),
                    message.getRecipientAccount(), message.getFailureReason());
        }
        // In production: call an email provider (SendGrid, AWS SES, etc.)
    }
}
