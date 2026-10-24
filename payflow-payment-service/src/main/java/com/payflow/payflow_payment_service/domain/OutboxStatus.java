package com.payflow.payflow_payment_service.domain;

public enum OutboxStatus {
    PENDING,    // created, not yet published to Kafka
    PUBLISHED,  // successfully published to Kafka
    FAILED      // publishing failed after max retries — needs manual intervention
}
