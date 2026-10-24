package com.payflow.payflow_processor_service.repository;

import com.payflow.payflow_processor_service.domain.Payment;
import com.payflow.payflow_processor_service.domain.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    // Atomic status transition with optimistic lock check baked in by JPA @Version.
    // Using @Modifying + @Query for a direct UPDATE — avoids loading the entity
    // into memory just to change one field, which is more efficient under load.
    @Modifying
    @Query("UPDATE Payment p SET p.status = :newStatus WHERE p.id = :id AND p.status = :expectedStatus")
    int transitionStatus(UUID id, PaymentStatus expectedStatus, PaymentStatus newStatus);
}
