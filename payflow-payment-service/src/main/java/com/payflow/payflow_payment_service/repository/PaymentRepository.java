package com.payflow.payflow_payment_service.repository;

import com.payflow.payflow_payment_service.domain.Payment;
import com.payflow.payflow_payment_service.domain.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    // Used for idempotency check: before inserting, see if this key exists
    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    // Used by monitoring / admin queries
    List<Payment> findByStatus(PaymentStatus status);

    // CONCEPT — JPQL vs native SQL:
    // Spring Data generates the SQL from the method name (findBy...).
    // For complex queries, use @Query with JPQL (object-oriented) or
    // nativeQuery=true for raw SQL. JPQL is database-agnostic but
    // native SQL lets you use Postgres-specific features like JSONB operators.
    @Query("SELECT p FROM Payment p WHERE p.status = :status ORDER BY p.createdAt ASC")
    List<Payment> findByStatusOrderByCreatedAtAsc(PaymentStatus status);
}
