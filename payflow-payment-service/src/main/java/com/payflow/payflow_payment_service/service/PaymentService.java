package com.payflow.payflow_payment_service.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.payflow.payflow_payment_service.domain.OutboxEvent;
import com.payflow.payflow_payment_service.domain.OutboxStatus;
import com.payflow.payflow_payment_service.domain.Payment;
import com.payflow.payflow_payment_service.domain.PaymentStatus;
import com.payflow.payflow_payment_service.dto.PaymentEvent;
import com.payflow.payflow_payment_service.dto.PaymentRequest;
import com.payflow.payflow_payment_service.dto.PaymentResponse;
import com.payflow.payflow_payment_service.exception.DuplicatePaymentException;
import com.payflow.payflow_payment_service.exception.PaymentNotFoundException;
import com.payflow.payflow_payment_service.repository.OutboxEventRepository;
import com.payflow.payflow_payment_service.repository.PaymentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    // Redis key prefix — prevents collisions with other keys in Redis
    private static final String IDEMPOTENCY_CACHE_PREFIX = "payment:idempotency:";
    // How long to cache idempotency results — 24h is typical
    private static final Duration IDEMPOTENCY_TTL = Duration.ofHours(24);

    /**
     * CONCEPT — The full idempotency flow:
     *
     * Layer 1: Redis cache (fastest — sub-millisecond)
     *   If we've seen this key before, return the cached response immediately.
     *   This handles the 99% case of retries without touching the DB at all.
     *
     * Layer 2: Database unique constraint (safety net)
     *   If Redis is down or the cache expired, the UNIQUE constraint on
     *   idempotency_key in the DB is the last line of defence.
     *   A duplicate INSERT will throw a DataIntegrityViolationException
     *   which we catch and convert to a 409 Conflict.
     *
     * CONCEPT — @Transactional:
     *   The @Transactional annotation wraps the method in a DB transaction.
     *   If ANY exception is thrown, the transaction rolls back — both the
     *   payments INSERT and the outbox_events INSERT are rolled back together.
     *   This is the "atomic" part of the Outbox Pattern.
     *
     *   Default behaviour: rolls back on RuntimeException and Error only.
     *   For checked exceptions, add rollbackFor = Exception.class if needed.
     */
    @Transactional
    public PaymentResponse initiatePayment(PaymentRequest request) {
        String cacheKey = IDEMPOTENCY_CACHE_PREFIX + request.getIdempotencyKey();

        // ── Layer 1: Redis idempotency check ──────────────────────────────────
        String cachedResponse = redisTemplate.opsForValue().get(cacheKey);
        if (cachedResponse != null) {
            log.info("Idempotency hit (Redis cache) for key: {}", request.getIdempotencyKey());
            try {
                return objectMapper.readValue(cachedResponse, PaymentResponse.class);
            } catch (JsonProcessingException e) {
                // Cache is corrupt — fall through to DB check
                log.warn("Failed to deserialize cached response for key: {}", request.getIdempotencyKey());
            }
        }

        // ── Layer 2: DB idempotency check ─────────────────────────────────────
        // Handles the case where Redis is down, cache expired, or first run
        var existingPayment = paymentRepository.findByIdempotencyKey(request.getIdempotencyKey());
        if (existingPayment.isPresent()) {
            log.info("Idempotency hit (DB) for key: {}", request.getIdempotencyKey());
            PaymentResponse response = PaymentResponse.fromEntity(existingPayment.get());
            cacheResponse(cacheKey, response);
            return response;
        }

        // ── Create the payment ────────────────────────────────────────────────
        Payment payment = Payment.builder()
                .idempotencyKey(request.getIdempotencyKey())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .senderAccount(request.getSenderAccount())
                .receiverAccount(request.getReceiverAccount())
                .status(PaymentStatus.PENDING)
                .build();

        payment = paymentRepository.save(payment);
        log.info("Payment created: id={}, idempotencyKey={}", payment.getId(), payment.getIdempotencyKey());

        // ── Write to Outbox (same transaction — atomic with payment insert) ──
        // If Kafka is down, the payment is still saved. The outbox poller
        // will retry publishing until Kafka comes back up.
        createOutboxEvent(payment, "PAYMENT_INITIATED");

        PaymentResponse response = PaymentResponse.fromEntity(payment);
        cacheResponse(cacheKey, response);

        return response;
    }

    public PaymentResponse getPayment(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .map(PaymentResponse::fromEntity)
                .orElseThrow(() -> new PaymentNotFoundException(paymentId));
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /**
     * Creates an outbox event for the given payment.
     * Called within an active @Transactional context — this INSERT happens
     * in the same transaction as the payment INSERT.
     */
    private void createOutboxEvent(Payment payment, String eventType) {
        PaymentEvent event = PaymentEvent.builder()
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
                .build();

        String payload;
        try {
            payload = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize payment event", e);
        }

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId(payment.getId())
                .eventType(eventType)
                .payload(payload)
                .status(OutboxStatus.PENDING)
                .build();

        outboxEventRepository.save(outboxEvent);
        log.debug("Outbox event created: type={}, aggregateId={}", eventType, payment.getId());
    }

    private void cacheResponse(String cacheKey, PaymentResponse response) {
        try {
            String json = objectMapper.writeValueAsString(response);
            redisTemplate.opsForValue().set(cacheKey, json, IDEMPOTENCY_TTL);
        } catch (JsonProcessingException e) {
            // Non-fatal — the DB is the source of truth, Redis is just a cache
            log.warn("Failed to cache idempotency response: {}", e.getMessage());
        }
    }
}
