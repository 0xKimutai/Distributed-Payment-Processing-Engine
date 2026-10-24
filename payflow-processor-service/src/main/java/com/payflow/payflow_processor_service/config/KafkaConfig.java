package com.payflow.payflow_processor_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * CONCEPT — Manual Acknowledgment Mode (AckMode.MANUAL):
 *
 * Spring Kafka's ContainerProperties.AckMode controls WHEN the consumer
 * commits its offset back to Kafka.
 *
 * AckMode.BATCH (default auto-commit):
 *   Commits after processing each batch. If your service crashes mid-batch,
 *   some messages may be redelivered. But you don't control exactly when.
 *
 * AckMode.MANUAL:
 *   You call ack.acknowledge() explicitly in your listener method.
 *   Commit happens ONLY after you've successfully processed the message.
 *   Crash before ack → Kafka redelivers → your idempotency check handles it.
 *   This is the correct mode for financial systems.
 *
 * CONCEPT — ConcurrentKafkaListenerContainerFactory:
 * The concurrency setting controls how many threads consume in parallel.
 * Set concurrency = number of partitions for maximum throughput.
 * (3 partitions → 3 threads → 3x throughput vs single-threaded)
 * More threads than partitions = idle threads, no benefit.
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${payflow.kafka.topics.payment-dlq}")
    private String dlqTopic;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // manual ack
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // CONCEPT — max.poll.records:
        // How many records Kafka returns in a single poll() call.
        // Lower = less memory, more frequent commits, better latency per message.
        // Higher = better throughput. Default is 500. We use 10 for a payment
        // processor since each message is expensive (external API call).
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        // CONCEPT — max.poll.interval.ms:
        // Kafka considers a consumer dead if it doesn't call poll() within this interval.
        // Since our processing can take up to 4s (retries + backoff), set this high enough.
        // If processing exceeds this, Kafka triggers a rebalance and reassigns partitions.
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000); // 60 seconds

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Manual ack — we control when offsets are committed
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // 3 concurrent threads = 1 per partition for our 3-partition topic
        factory.setConcurrency(3);

        return factory;
    }

    // Ensure DLQ topic exists
    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name(dlqTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
