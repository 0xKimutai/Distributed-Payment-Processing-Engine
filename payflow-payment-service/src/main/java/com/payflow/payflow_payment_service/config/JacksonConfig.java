package com.payflow.payflow_payment_service.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the ObjectMapper used across the application.
 * JavaTimeModule makes Jackson handle java.time types (Instant, LocalDate, etc.)
 * WRITE_DATES_AS_TIMESTAMPS=false means dates are serialized as ISO-8601 strings,
 * not epoch milliseconds — much more readable and interoperable.
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
