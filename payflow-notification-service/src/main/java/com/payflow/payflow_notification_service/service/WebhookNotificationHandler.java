package com.payflow.payflow_notification_service.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payflow_notification_service.dto.NotificationMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

/**
 * CONCEPT — Webhook pattern:
 *
 * Webhooks are HTTP callbacks to partner systems (e.g. a merchant's server).
 * When a payment completes, we POST the result to the merchant's configured URL.
 * The merchant's server processes it and responds with 200 OK.
 *
 * This is the "push" model — we push events to partners instead of them
 * polling our API. Used by Stripe, M-Pesa, Paystack for partner integrations.
 *
 * Reliability concern: the partner server might be down. In production you'd:
 * 1. Retry with exponential backoff (same pattern as the processor)
 * 2. If max retries exceeded → nack the RabbitMQ message → goes to DLQ
 * 3. Ops team inspects DLQ, partner is notified, manual replay possible
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class WebhookNotificationHandler {

    private final ObjectMapper objectMapper;

    // Only receives payment.completed
    @RabbitListener(queues = "${payflow.rabbitmq.queues.webhook}")
    public void handleWebhookNotification(NotificationMessage message) {
        log.info("[WEBHOOK] Dispatching webhook for payment={}", message.getPaymentId());

        try {
            String payload = objectMapper.writeValueAsString(message);
            // In production: RestTemplate/WebClient POST to merchant's webhook URL
            log.info("[WEBHOOK] ✅ Would POST to merchant webhook: {}", payload);
        } catch (Exception e) {
            log.error("[WEBHOOK] Failed to dispatch webhook for payment={}: {}",
                    message.getPaymentId(), e.getMessage());
            // Throwing here causes Spring AMQP to nack the message → routes to DLQ
            throw new RuntimeException("Webhook dispatch failed", e);
        }
    }
}
