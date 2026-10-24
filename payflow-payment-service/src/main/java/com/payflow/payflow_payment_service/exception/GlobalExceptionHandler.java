package com.payflow.payflow_payment_service.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.time.Instant;
import java.util.stream.Collectors;

/**
 * CONCEPT — RFC 7807 Problem Details:
 * Instead of custom error response shapes, we use the standard ProblemDetail
 * format (RFC 7807), which Spring 6 supports natively.
 * This gives consistent, machine-readable error responses:
 * {
 *   "type": "https://payflow.com/errors/not-found",
 *   "title": "Payment Not Found",
 *   "status": 404,
 *   "detail": "Payment not found: ..."
 * }
 * Clients and API consumers can rely on this shape universally.
 */
@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(PaymentNotFoundException.class)
    public ProblemDetail handleNotFound(PaymentNotFoundException ex) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
        problem.setType(URI.create("https://payflow.com/errors/not-found"));
        problem.setTitle("Payment Not Found");
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(DuplicatePaymentException.class)
    public ProblemDetail handleDuplicate(DuplicatePaymentException ex) {
        // 409 Conflict — the idempotency key was reused with different parameters
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, ex.getMessage());
        problem.setType(URI.create("https://payflow.com/errors/duplicate-payment"));
        problem.setTitle("Duplicate Payment");
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    // Triggered by @Valid failures on request body
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        String details = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
                .collect(Collectors.joining(", "));

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, details);
        problem.setType(URI.create("https://payflow.com/errors/validation"));
        problem.setTitle("Validation Failed");
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneral(Exception ex) {
        log.error("Unhandled exception", ex);
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
        problem.setType(URI.create("https://payflow.com/errors/internal"));
        problem.setTitle("Internal Server Error");
        problem.setProperty("timestamp", Instant.now());
        return problem;
    }
}
