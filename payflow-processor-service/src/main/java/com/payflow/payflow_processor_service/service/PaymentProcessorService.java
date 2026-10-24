package com.payflow.payflow_processor_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payflow_processor_service.domain.OutboxEvent;
import com.payflow.payflow_processor_service.domain.OutboxStatus;
import com.payflow.payflow_processor_service.domain.Payment;
import com.payflow.payflow_processor_service.domain.PaymentStatus;
import com.payflow.payflow_processor_service.dto.PaymentEvent;
import com.payflow.payflow_processor_service.provider.PaymentProviderResponse;
import com.payflow.payflow_processor_service.provider.SimulatedPaymentProvider;
import com.payflow.payflow_processor_service.repository.OutboxEventRepository;
import com.payflow.payflow_processor_service.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * CONCEPT — The Processor's Responsibilities:
 *
 * 1. Transition payment PENDING → PROCESSING (claim the payment)
 * 2. Call the external provider
 * 3. Transition PROCESSING → COMPLETED or FAILED
 * 4. Write outbox event for the result (same transaction as step 3)
 *
 * CONCURRENCY SAFETY:
 * If two processor instances consume the same Kafka message (possible with
 * at-least-once delivery), both try step 1. The transitionStatus query:
 *   UPDATE payments SET status='PROCESSING' WHERE id=? AND status='PENDING'
 * ...only affects 1 row for the first instance. The second gets rowsAffected=0
 * → it knows the payment is already being processed and skips it.
 * This is a "compare-and-swap" at the DB level — a classic distributed locking pattern.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentProcessorService {

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final SimulatedPaymentProvider paymentProvider;
    private final ObjectMapper objectMapper;

    /**
     * Main processing method — called by the Kafka listener.
     * Returns true if processed, false if skipped (already processed or not found).
     */
    @Transactional
    public boolean processPayment(PaymentEvent event) {
        UUID paymentId = event.getPaymentId();
        log.info("Processing payment: id={}, eventId={}", paymentId, event.getEventId());

        // ── Step 1: Claim the payment (PENDING → PROCESSING) ─────────────────
        // CONCEPT — Atomic compare-and-swap:
        // This UPDATE only succeeds if status is currently PENDING.
        // If two consumers race, only one wins. The other gets rowsUpdated=0.
        int rowsUpdated = paymentRepository.transitionStatus(
                paymentId, PaymentStatus.PENDING, PaymentStatus.PROCESSING);

        if (rowsUpdated == 0) {
            // Payment either doesn't exist or was already claimed by another consumer.
            // This is normal with at-least-once delivery — not an error.
            log.warn("Payment {} already processing or not found — skipping", paymentId);
            return false;
        }

        // Load the full payment entity for the provider call
        Payment payment = paymentRepository.findById(paymentId)
                .orElseThrow(() -> new IllegalStateException("Payment disappeared after claiming: " + paymentId));

        // ── Step 2: Call the payment provider ────────────────────────────────
        PaymentProviderResponse providerResponse = paymentProvider.process(
                payment.getId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getSenderAccount(),
                payment.getReceiverAccount()
        );

        // ── Step 3 + 4: Update status and write outbox (atomic) ──────────────
        if (providerResponse.isSuccess()) {
            completePayment(payment, providerResponse.getProviderRef());
        } else {
            failPayment(payment, providerResponse.getFailureReason());
        }

        return true;
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    private void completePayment(Payment payment, String providerRef) {
        payment.setStatus(PaymentStatus.COMPLETED);
        payment.setProviderRef(providerRef);
        payment.setUpdatedAt(Instant.now());
        paymentRepository.save(payment);

        writeOutboxEvent(payment, "PAYMENT_COMPLETED");
        log.info("Payment completed: id={}, providerRef={}", payment.getId(), providerRef);
    }

    private void failPayment(Payment payment, String failureReason) {
        payment.setStatus(PaymentStatus.FAILED);
        payment.setFailureReason(failureReason);
        payment.setUpdatedAt(Instant.now());
        paymentRepository.save(payment);

        writeOutboxEvent(payment, "PAYMENT_FAILED");
        log.warn("Payment failed: id={}, reason={}", payment.getId(), failureReason);
    }

    private void writeOutboxEvent(Payment payment, String eventType) {
        PaymentEvent resultEvent = PaymentEvent.builder()
                .eventId(UUID.randomUUID())
                .eventType(eventType)
                .occurredAt(Instant.now())
                .paymentId(payment.getId())
                .idempotencyKey(payment.getIdempotencyKey())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .senderAccount(payment.getSenderAccount())
                .receiverAccount(payment.getReceiverAccount())
                .status(payment.getStatus().name())
                .providerRef(payment.getProviderRef())
                .failureReason(payment.getFailureReason())
                .build();

        String payload;
        try {
            payload = objectMapper.writeValueAsString(resultEvent);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize event for payment: " + payment.getId(), e);
        }

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId(payment.getId())
                .eventType(eventType)
                .payload(payload)
                .status(OutboxStatus.PENDING)
                .build();

        outboxEventRepository.save(outboxEvent);
    }
}
