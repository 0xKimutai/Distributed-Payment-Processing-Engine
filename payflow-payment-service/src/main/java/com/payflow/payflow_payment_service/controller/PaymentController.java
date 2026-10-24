package com.payflow.payflow_payment_service.controller;

import com.payflow.payflow_payment_service.dto.PaymentRequest;
import com.payflow.payflow_payment_service.dto.PaymentResponse;
import com.payflow.payflow_payment_service.service.PaymentService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * CONCEPT — 202 Accepted vs 200 OK vs 201 Created:
 *
 * For async payment processing, we return 202 Accepted — NOT 200 OK.
 * This tells the client: "I've received your request and it's queued,
 * but it hasn't been completed yet. Poll /payments/{id} to check status."
 *
 * 200 OK = synchronous, the work is done
 * 201 Created = synchronous, a resource was created
 * 202 Accepted = async, request accepted, processing is happening in background
 *
 * This is a key REST semantics distinction interviewers ask about.
 */
@RestController
@RequestMapping("/api/v1/payments")
@RequiredArgsConstructor
@Slf4j
public class PaymentController {

    private final PaymentService paymentService;

    /**
     * POST /api/v1/payments
     * Initiates a new payment. Idempotent — safe to retry with the same idempotencyKey.
     */
    @PostMapping
    public ResponseEntity<PaymentResponse> initiatePayment(
            @Valid @RequestBody PaymentRequest request) {

        log.info("Payment request received: idempotencyKey={}, amount={} {}",
                request.getIdempotencyKey(), request.getAmount(), request.getCurrency());

        PaymentResponse response = paymentService.initiatePayment(request);

        // If payment already existed (idempotency hit), return 200 OK
        // If newly created, return 202 Accepted
        HttpStatus status = response.getStatus().name().equals("PENDING")
                && response.getCreatedAt().equals(response.getUpdatedAt())
                ? HttpStatus.ACCEPTED
                : HttpStatus.OK;

        return ResponseEntity.status(status).body(response);
    }

    /**
     * GET /api/v1/payments/{id}
     * Polls the status of a payment. Client uses this after receiving 202.
     */
    @GetMapping("/{id}")
    public ResponseEntity<PaymentResponse> getPayment(@PathVariable UUID id) {
        return ResponseEntity.ok(paymentService.getPayment(id));
    }
}
