package com.payflow.payflow_payment_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

// @EnableScheduling activates the @Scheduled annotation used by OutboxPoller
@SpringBootApplication
@EnableScheduling
public class PayflowPaymentServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(PayflowPaymentServiceApplication.class, args);
	}

}
