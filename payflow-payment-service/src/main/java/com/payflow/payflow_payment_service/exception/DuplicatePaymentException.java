package com.payflow.payflow_payment_service.exception;

public class DuplicatePaymentException extends RuntimeException {
    public DuplicatePaymentException(String idempotencyKey) {
        super("Payment with idempotency key already exists: " + idempotencyKey);
    }
}
