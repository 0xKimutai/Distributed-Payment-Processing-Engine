package com.payflow.payflow_notification_service.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * CONCEPT — RabbitMQ Topology: Exchanges, Queues, Bindings
 *
 * The three entities of RabbitMQ:
 *
 * 1. EXCHANGE
 *    Receives messages from producers. Does NOT store messages.
 *    Routes messages to zero or more queues based on routing rules.
 *    Think of it as a post office sorting room.
 *
 * 2. QUEUE
 *    Stores messages until consumed. Messages are deleted after delivery.
 *    (Unlike Kafka which retains messages for days/weeks regardless.)
 *
 * 3. BINDING
 *    A rule linking an exchange to a queue, with an optional routing key pattern.
 *    "Messages matching routing key 'payment.*' go to email.queue"
 *
 * ────────────────────────────────────────────────────────────────────
 * THE FOUR EXCHANGE TYPES:
 *
 * DIRECT exchange:
 *   Routes to queues where bindingKey == routingKey (exact match)
 *   Use case: task queues, one producer → one consumer type
 *   Example: routingKey="order.process" → only the order-processing queue
 *
 * TOPIC exchange (what we use):
 *   Routes by pattern matching on routing key
 *   * = exactly one word,  # = zero or more words
 *   Use case: selective fan-out — different consumers want different events
 *   Example:
 *     "payment.completed" → matches "payment.*" → email, sms, webhook queues
 *     "payment.failed"    → matches "payment.*" → email queue only
 *
 * FANOUT exchange:
 *   Ignores routing key. Sends to ALL bound queues.
 *   Use case: pure broadcast — every consumer gets every message
 *   Example: system-wide announcements, cache invalidation
 *
 * HEADERS exchange:
 *   Routes based on message header attributes, not routing key
 *   Use case: complex routing logic that doesn't map to a string key
 *   Example: route based on {region: "KE", priority: "high"}
 * ────────────────────────────────────────────────────────────────────
 *
 * OUR TOPOLOGY:
 *
 *  Kafka: payment.completed ──► [NotificationListener]
 *  Kafka: payment.failed    ──► [NotificationListener]
 *                                        │
 *                                        ▼
 *                            [payment.results TOPIC EXCHANGE]
 *                              │                │              │
 *               binding:       │                │              │
 *               payment.*      │   payment.*    │  payment.*   │
 *                              ▼                ▼              ▼
 *                       [email.q]         [sms.q]       [webhook.q]
 *                          ↑                  ↑
 *               payment.completed         payment.completed only
 *               payment.failed            (payment.failed NOT bound to sms/webhook)
 *
 * WHY topic exchange over fanout?
 *   Fanout sends EVERYTHING to ALL queues — both completed AND failed go to sms.
 *   With topic exchange, we bind sms.queue to "payment.completed" only.
 *   Users don't get an SMS when their payment fails — they get an email.
 *   Adding push notifications later = add a queue + binding. Zero existing code changes.
 */
@Configuration
public class RabbitMQConfig {

    @Value("${payflow.rabbitmq.exchange}")
    private String exchangeName;

    @Value("${payflow.rabbitmq.queues.email}")
    private String emailQueue;

    @Value("${payflow.rabbitmq.queues.sms}")
    private String smsQueue;

    @Value("${payflow.rabbitmq.queues.webhook}")
    private String webhookQueue;

    @Value("${payflow.rabbitmq.routing-keys.completed}")
    private String completedRoutingKey;

    @Value("${payflow.rabbitmq.routing-keys.failed}")
    private String failedRoutingKey;

    // ── Exchange ─────────────────────────────────────────────────────────────
    @Bean
    public TopicExchange paymentResultsExchange() {
        // durable=true: exchange survives RabbitMQ restart
        // autoDelete=false: don't delete when last consumer disconnects
        return new TopicExchange(exchangeName, true, false);
    }

    // ── Queues ───────────────────────────────────────────────────────────────
    @Bean
    public Queue emailQueue() {
        // durable=true: queue survives RabbitMQ restart. Messages in it are not lost.
        // If durable=false: queue disappears on restart — all unprocessed messages lost.
        return QueueBuilder.durable(emailQueue)
                // CONCEPT — Dead Letter Exchange for RabbitMQ:
                // If a message is rejected (nack) or expires, route it to this DLX
                // instead of dropping it. Same concept as Kafka DLQ but RabbitMQ-native.
                .withArgument("x-dead-letter-exchange", exchangeName + ".dlx")
                .withArgument("x-dead-letter-routing-key", "dead." + emailQueue)
                .build();
    }

    @Bean
    public Queue smsQueue() {
        return QueueBuilder.durable(smsQueue)
                .withArgument("x-dead-letter-exchange", exchangeName + ".dlx")
                .withArgument("x-dead-letter-routing-key", "dead." + smsQueue)
                .build();
    }

    @Bean
    public Queue webhookQueue() {
        return QueueBuilder.durable(webhookQueue)
                .withArgument("x-dead-letter-exchange", exchangeName + ".dlx")
                .withArgument("x-dead-letter-routing-key", "dead." + webhookQueue)
                .build();
    }

    // ── DLX (Dead Letter Exchange) + DLQs ────────────────────────────────────
    // CONCEPT — RabbitMQ DLQ:
    // When a message is nack'd (negatively acknowledged) without requeue,
    // RabbitMQ routes it to the dead letter exchange (DLX).
    // The DLX routes it to a dedicated DLQ for inspection.
    // Same purpose as Kafka's DLQ — sideline bad messages, keep normal flow running.
    @Bean
    public DirectExchange deadLetterExchange() {
        return new DirectExchange(exchangeName + ".dlx", true, false);
    }

    @Bean
    public Queue emailDlq() {
        return QueueBuilder.durable("email.notifications.dlq").build();
    }

    @Bean
    public Queue smsDlq() {
        return QueueBuilder.durable("sms.notifications.dlq").build();
    }

    @Bean
    public Queue webhookDlq() {
        return QueueBuilder.durable("webhook.notifications.dlq").build();
    }

    // ── Bindings ─────────────────────────────────────────────────────────────
    // CONCEPT — Binding = the routing rule between exchange and queue.
    // Topic exchange uses pattern matching:
    //   "payment.*" matches "payment.completed" and "payment.failed"
    //   "payment.completed" matches only "payment.completed" (exact)

    // Email gets BOTH completed and failed events
    @Bean
    public Binding emailCompletedBinding() {
        return BindingBuilder.bind(emailQueue())
                .to(paymentResultsExchange())
                .with(completedRoutingKey);   // "payment.completed"
    }

    @Bean
    public Binding emailFailedBinding() {
        return BindingBuilder.bind(emailQueue())
                .to(paymentResultsExchange())
                .with(failedRoutingKey);       // "payment.failed"
    }

    // SMS gets only completed (success notification)
    @Bean
    public Binding smsCompletedBinding() {
        return BindingBuilder.bind(smsQueue())
                .to(paymentResultsExchange())
                .with(completedRoutingKey);
    }

    // Webhook gets only completed (partner system integration)
    @Bean
    public Binding webhookCompletedBinding() {
        return BindingBuilder.bind(webhookQueue())
                .to(paymentResultsExchange())
                .with(completedRoutingKey);
    }

    // DLX bindings
    @Bean
    public Binding emailDlqBinding() {
        return BindingBuilder.bind(emailDlq())
                .to(deadLetterExchange())
                .with("dead." + emailQueue);
    }

    @Bean
    public Binding smsDlqBinding() {
        return BindingBuilder.bind(smsDlq())
                .to(deadLetterExchange())
                .with("dead." + smsQueue);
    }

    @Bean
    public Binding webhookDlqBinding() {
        return BindingBuilder.bind(webhookDlq())
                .to(deadLetterExchange())
                .with("dead." + webhookQueue);
    }

    // ── Message Converter ─────────────────────────────────────────────────────
    // CONCEPT — MessageConverter:
    // By default RabbitMQ serializes Java objects as binary (Java serialization).
    // Jackson2JsonMessageConverter serializes as JSON — readable, language-agnostic.
    // Any consumer (Python, Go, Node) can read the message.
    // Always use JSON in a polyglot microservices environment.
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter());
        // CONCEPT — Mandatory + ConfirmCallback:
        // If a message is published to an exchange but NO queue is bound
        // to receive it (routing key matches nothing), the message is silently dropped.
        // Setting mandatory=true causes RabbitMQ to return the message to the publisher
        // so you can handle it (log, DLQ, retry).
        template.setMandatory(true);
        return template;
    }
}
