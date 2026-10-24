package com.payflow.payflow_payment_service.dto;

import com.payflow.payflow_payment_service.domain.Payment;
import com.payflow.payflow_payment_service.domain.PaymentStatus;
import lombok.Builder;
import lombok.Data;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
@Builder
public class PaymentResponse {

    private UUID id;
    private String idempotencyKey;
    private BigDecimal amount;
    private String currency;
    private String senderAccount;
    private String receiverAccount;
    private PaymentStatus status;
    private String providerRef;
    private String failureReason;
    private Instant createdAt;
    private Instant updatedAt;

    // CONCEPT — Static factory vs MapStruct:
    // For a learning project, a static fromEntity() method is clear and simple.
    // In production with many DTOs, use MapStruct — it generates mapping code
    // at compile time (zero reflection overhead) and is safer to maintain.
    public static PaymentResponse fromEntity(Payment payment) {
        return PaymentResponse.builder()
                .id(payment.getId())
                .idempotencyKey(payment.getIdempotencyKey())
                .amount(payment.getAmount())
                .currency(payment.getCurrency())
                .senderAccount(payment.getSenderAccount())
                .receiverAccount(payment.getReceiverAccount())
                .status(payment.getStatus())
                .providerRef(payment.getProviderRef())
                .failureReason(payment.getFailureReason())
                .createdAt(payment.getCreatedAt())
                .updatedAt(payment.getUpdatedAt())
                .build();
    }
}
