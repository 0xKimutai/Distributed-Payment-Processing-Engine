package com.payflow.payflow_processor_service.provider;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Random;
import java.util.UUID;

/**
 * CONCEPT — Simulated External Dependency (Anti-Corruption Layer):
 *
 * This class stands in for a real payment provider (M-Pesa, bank API, etc.).
 * It models the key behaviours of real external payment systems:
 *
 * 1. LATENCY — real APIs take 200ms–3s. We simulate this with Thread.sleep.
 *    This is why async processing matters — you can't block an HTTP thread
 *    for 3 seconds per payment at scale.
 *
 * 2. PARTIAL FAILURE — real APIs fail ~1-5% of the time. We simulate this
 *    with a configurable failure rate. This is what forces you to design
 *    retries, DLQs, and idempotency into the processor.
 *
 * 3. TRANSIENT vs PERMANENT failures:
 *    - Transient: "provider temporarily unavailable" → safe to retry
 *    - Permanent: "invalid account number" → retrying won't help, send to DLQ
 *    Real systems distinguish these by HTTP status code (503 vs 400).
 *    We simulate both types here.
 *
 * WHY this pattern matters in an interview:
 * "How do you handle calls to external payment APIs?"
 * Answer: wrap them in a circuit breaker (Resilience4j), retry with
 * exponential backoff for transient errors, and immediately DLQ permanent errors.
 */
@Component
@Slf4j
public class SimulatedPaymentProvider {

    @Value("${payflow.processor.simulated-failure-rate:0.2}")
    private double failureRate;

    private final Random random = new Random();

    public PaymentProviderResponse process(UUID paymentId, BigDecimal amount, String currency,
                                           String senderAccount, String receiverAccount) {

        log.info("Calling simulated payment provider for payment: {}", paymentId);

        // Simulate network latency (200ms - 800ms)
        simulateLatency();

        // Simulate transient failure (e.g. provider timeout, 503)
        if (random.nextDouble() < failureRate) {
            String reason = simulateFailureReason();
            log.warn("Simulated provider failure for payment {}: {}", paymentId, reason);
            return PaymentProviderResponse.builder()
                    .success(false)
                    .failureReason(reason)
                    .build();
        }

        // Success — generate a fake provider reference
        String providerRef = "SIM-" + Instant.now().toEpochMilli() + "-" +
                UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        log.info("Simulated provider success for payment {}: ref={}", paymentId, providerRef);

        return PaymentProviderResponse.builder()
                .success(true)
                .providerRef(providerRef)
                .build();
    }

    private void simulateLatency() {
        try {
            long latencyMs = 200 + (long) (random.nextDouble() * 600);
            Thread.sleep(latencyMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String simulateFailureReason() {
        // Mix of transient and permanent errors — same as real providers
        String[] reasons = {
                "PROVIDER_TIMEOUT",           // transient — retry
                "PROVIDER_UNAVAILABLE",        // transient — retry
                "INSUFFICIENT_FUNDS",          // permanent — don't retry
                "INVALID_ACCOUNT",             // permanent — don't retry
                "DAILY_LIMIT_EXCEEDED"         // permanent — don't retry
        };
        return reasons[random.nextInt(reasons.length)];
    }

    /**
     * Whether this failure reason is transient (safe to retry) or permanent.
     * In a real system, HTTP 5xx = transient, HTTP 4xx = permanent.
     */
    public static boolean isTransient(String failureReason) {
        return failureReason != null && (
                failureReason.equals("PROVIDER_TIMEOUT") ||
                failureReason.equals("PROVIDER_UNAVAILABLE")
        );
    }
}
