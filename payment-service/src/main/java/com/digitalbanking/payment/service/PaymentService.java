package com.digitalbanking.payment.service;

import com.digitalbanking.payment.dto.PaymentRequest;
import com.digitalbanking.payment.entity.Payment;
import com.digitalbanking.payment.repository.PaymentRepository;
import org.springframework.stereotype.Service;
import com.digitalbanking.payment.client.AccountClient;
import com.digitalbanking.payment.exception.AccountNotFoundException;
import com.digitalbanking.payment.exception.PaymentNotFoundException;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final AccountClient accountClient;

    public PaymentService(PaymentRepository paymentRepository,AccountClient accountClient) {
        this.paymentRepository = paymentRepository;
        this.accountClient = accountClient;
    }

    public Payment createPayment(PaymentRequest request) {

        // Idempotency check
        Payment existingPayment = paymentRepository
                .findByIdempotencyKey(request.getIdempotencyKey())
                .orElse(null);

        if (existingPayment != null) {
            return existingPayment;
        }

        if (!accountClient.accountExists(request.getSourceAccountNumber())) {
            throw new AccountNotFoundException(
                    "Source account not found: " + request.getSourceAccountNumber()
            );
        }

        if (!accountClient.accountExists(request.getTargetAccountNumber())) {
            throw new AccountNotFoundException(
                    "Target account not found: " + request.getTargetAccountNumber()
            );
        }

        LocalDateTime now = LocalDateTime.now();

        Payment payment = Payment.builder()
                .paymentReference("PAY-" + UUID.randomUUID())
                .sourceAccountNumber(request.getSourceAccountNumber())
                .targetAccountNumber(request.getTargetAccountNumber())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .paymentType(request.getPaymentType())
                .status("PENDING")
                .idempotencyKey(request.getIdempotencyKey())
                .createdAt(now)
                .updatedAt(now)
                .build();

        return paymentRepository.save(payment);
    }

    public List<Payment> getAllPayments() {
        return paymentRepository.findAll();
    }

    public Payment getPaymentByReference(String paymentReference) {
        return paymentRepository.findByPaymentReference(paymentReference)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: " + paymentReference
                        )
                );
    }
}