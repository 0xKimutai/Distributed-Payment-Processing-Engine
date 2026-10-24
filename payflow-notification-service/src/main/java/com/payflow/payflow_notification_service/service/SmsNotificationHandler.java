package com.payflow.payflow_notification_service.service;

import com.payflow.payflow_notification_service.dto.NotificationMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * CONCEPT — Selective routing via bindings:
 *
 * This queue is bound to "payment.completed" ONLY (see RabbitMQConfig).
 * Even though both completed and failed events are published to the exchange,
 * ONLY completed events arrive here. Failed events are NOT routed here.
 *
 * This is the power of the topic exchange — each consumer gets exactly
 * the events it needs, without any filtering logic in the consumer code.
 * The routing rule lives in the infrastructure (binding), not the application.
 *
 * This is the Open/Closed Principle at the infrastructure level:
 * open for extension (add a new queue + binding), closed for modification
 * (existing handlers don't change when you add push notifications).
 */
@Component
@Slf4j
public class SmsNotificationHandler {

    // Only receives payment.completed (per binding — payment.failed is NOT bound to this queue)
    @RabbitListener(queues = "${payflow.rabbitmq.queues.sms}")
    public void handleSmsNotification(NotificationMessage message) {
        log.info("[SMS] Sending success SMS for payment={}, recipient={}",
                message.getPaymentId(), message.getRecipientAccount());

        log.info("[SMS] ✅ Your payment of {} {} was successful. Ref: {}",
                message.getAmount(), message.getCurrency(), message.getProviderRef());
        // In production: call Africa's Talking, Twilio, etc.
    }
}
