package com.payflow.payflow_processor_service.domain;

// Mirrored from payment-service.
// In a real multi-repo setup you'd share this via a common library (payflow-common).
// Here we keep it simple and duplicate it — perfectly acceptable in microservices
// where services are meant to be independently deployable.
public enum PaymentStatus {
    PENDING,
    PROCESSING,
    COMPLETED,
    FAILED
}
