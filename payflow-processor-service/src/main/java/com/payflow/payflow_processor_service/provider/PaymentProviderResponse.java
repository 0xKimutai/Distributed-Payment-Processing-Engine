package com.payflow.payflow_processor_service.provider;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PaymentProviderResponse {
    private boolean success;
    private String providerRef;   // e.g. "MPESA-20241017-XYZ123"
    private String failureReason; // populated when success=false
}
