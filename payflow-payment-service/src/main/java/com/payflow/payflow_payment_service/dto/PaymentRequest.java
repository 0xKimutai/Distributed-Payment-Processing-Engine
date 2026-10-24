package com.payflow.payflow_payment_service.dto;

import jakarta.validation.constraints.*;
import lombok.Data;
import java.math.BigDecimal;

/**
 * CONCEPT — Idempotency Key in the API:
 *
 * The client is responsible for generating a unique idempotency key
 * (typically a UUID) and sending it with every payment request.
 *
 * If the client sends the request, the network times out, and it retries —
 * it must send the EXACT SAME idempotency key.
 *
 * The server then:
 *   1. Checks Redis: key exists? Return cached 200 response.
 *   2. Checks DB: payment with this key exists? Return it.
 *   3. Neither? Create the payment.
 *
 * This makes the operation safe to retry any number of times.
 * The client never needs to wonder "did my payment go through?"
 * It just retries until it gets a definitive success or failure.
 *
 * Real-world examples:
 *   - Stripe uses X-Idempotency-Key header
 *   - M-Pesa uses OriginatorConversationID
 *   - Paystack uses idempotencyKey in body
 *
 * We put it in the request body here for simplicity.
 * In production, it's more common as a header.
 */
@Data
public class PaymentRequest {

    @NotBlank(message = "Idempotency key is required")
    @Size(min = 10, max = 255, message = "Idempotency key must be between 10 and 255 characters")
    private String idempotencyKey;

    @NotNull(message = "Amount is required")
    @DecimalMin(value = "0.01", message = "Amount must be greater than 0")
    @Digits(integer = 15, fraction = 4, message = "Amount must have at most 15 integer digits and 4 decimal places")
    private BigDecimal amount;

    @NotBlank(message = "Currency is required")
    @Pattern(regexp = "^[A-Z]{3}$", message = "Currency must be a 3-letter ISO code (e.g. KES, USD)")
    private String currency;

    @NotBlank(message = "Sender account is required")
    @Pattern(regexp = "^ACC-\\d{3}$", message = "Sender account must be a valid account number e.g. ACC-001")
    private String senderAccount;

    @NotBlank(message = "Receiver account is required")
    @Pattern(regexp = "^ACC-\\d{3}$", message = "Receiver account must be a valid account number e.g. ACC-016")
    private String receiverAccount;
}
