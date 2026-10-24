package com.payflow.payflow_payment_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * CONCEPT — Kafka Topic Configuration:
 *
 * Topics are the fundamental unit of organisation in Kafka.
 * Think of a topic as a category or feed name (like a table in a DB).
 *
 * PARTITIONS:
 * Each topic is split into N partitions (we use 3 here).
 * - More partitions = more parallelism (more consumer threads can work in parallel)
 * - Each partition is an ordered, immutable sequence of records
 * - A consumer group gets one partition per consumer instance
 *   (3 partitions → max 3 consumer instances can work in parallel)
 * - Ordering is guaranteed WITHIN a partition, not across partitions
 * - We key messages by paymentId → same payment always goes to same partition → ordered
 *
 * REPLICATION FACTOR:
 * How many brokers keep a copy of each partition.
 * - replicationFactor=1 → fine for local dev (we only have 1 broker)
 * - replicationFactor=3 → production standard (survives 1 broker failure)
 * - Never use 1 in production for critical topics
 *
 * RETENTION:
 * Kafka retains messages for a configurable period (default 7 days).
 * Unlike RabbitMQ (which deletes messages after delivery), Kafka keeps
 * messages so consumers can replay the event log from any point.
 * This is what makes Kafka an "event log" rather than just a message queue.
 */
@Configuration
public class KafkaConfig {

    @Value("${payflow.kafka.topics.payment-initiated}")
    private String paymentInitiatedTopic;

    @Value("${payflow.kafka.topics.payment-completed}")
    private String paymentCompletedTopic;

    @Value("${payflow.kafka.topics.payment-failed}")
    private String paymentFailedTopic;

    @Bean
    public NewTopic paymentInitiatedTopic() {
        return TopicBuilder.name(paymentInitiatedTopic)
                .partitions(3)
                .replicas(1)   // 1 for local dev; use 3 in production
                .build();
    }

    @Bean
    public NewTopic paymentCompletedTopic() {
        return TopicBuilder.name(paymentCompletedTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentFailedTopic() {
        return TopicBuilder.name(paymentFailedTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
