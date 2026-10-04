package com.digitalbanking.payment.service;

import com.digitalbanking.payment.client.AccountClient;
import com.digitalbanking.payment.dto.AccountResponse;
import com.digitalbanking.payment.dto.PaymentRequest;
import com.digitalbanking.payment.entity.Payment;
import com.digitalbanking.payment.exception.AccountNotFoundException;
import com.digitalbanking.payment.exception.PaymentNotFoundException;
import com.digitalbanking.payment.repository.PaymentRepository;
import com.digitalbanking.payment.entity.OutboxEvent;
import com.digitalbanking.payment.repository.OutboxEventRepository;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.springframework.transaction.annotation.Transactional;

import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final AccountClient accountClient;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public PaymentService(PaymentRepository paymentRepository,
                          AccountClient accountClient,
                      OutboxEventRepository outboxEventRepository,
                          ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.accountClient = accountClient;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public Payment createPayment(PaymentRequest request) {

        // 1. IDEMPOTENCY CHECK
        Payment existingPayment = paymentRepository
                .findByIdempotencyKey(request.getIdempotencyKey())
                .orElse(null);

        if (existingPayment != null) {
            return existingPayment;
        }

        // 2. VALIDATE SOURCE ACCOUNT
        if (!accountClient.accountExists(request.getSourceAccountNumber())) {
            throw new AccountNotFoundException(
                    "Source account not found: "
                            + request.getSourceAccountNumber()
            );
        }

        // 3. VALIDATE TARGET ACCOUNT
        if (!accountClient.accountExists(request.getTargetAccountNumber())) {
            throw new AccountNotFoundException(
                    "Target account not found: "
                            + request.getTargetAccountNumber()
            );
        }

        // 4. GET ACCOUNT DETAILS
        AccountResponse sourceAccount =
                accountClient.getAccountByAccountNumber(
                        request.getSourceAccountNumber()
                );

        AccountResponse targetAccount =
                accountClient.getAccountByAccountNumber(
                        request.getTargetAccountNumber()
                );

        LocalDateTime now = LocalDateTime.now();

        // 5. CREATE PAYMENT AS PENDING
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

        payment = paymentRepository.save(payment);

        // ===========================
        // 6. DEBIT SOURCE ACCOUNT
        // ===========================

        try {

            accountClient.debitAccount(
                    sourceAccount.getId(),
                    request.getAmount()
            );

        } catch (Exception ex) {

            payment.setStatus("FAILED");
            payment.setUpdatedAt(LocalDateTime.now());

            payment = paymentRepository.save(payment);

            saveOutboxEvent(payment, "PAYMENT_FAILED");

            return payment;
        }

        // ===========================
        // 7. CREDIT TARGET ACCOUNT
        // ===========================

        try {

            accountClient.creditAccount(
                    targetAccount.getId(),
                    request.getAmount()
            );

            payment.setStatus("COMPLETED");
            payment.setUpdatedAt(LocalDateTime.now());

            payment = paymentRepository.save(payment);

            saveOutboxEvent(payment, "PAYMENT_COMPLETED");

            return payment;

        } catch (Exception creditException) {

            // ===========================
            // 8. SAGA COMPENSATION
            // ===========================

            try {

                // Debit succeeded but credit failed.
                // Put the money back into source account.

                accountClient.creditAccount(
                        sourceAccount.getId(),
                        request.getAmount()
                );

                payment.setStatus("COMPENSATED");
                payment.setUpdatedAt(LocalDateTime.now());

                payment = paymentRepository.save(payment);

                saveOutboxEvent(payment, "PAYMENT_COMPENSATED");

                return payment;

            } catch (Exception compensationException) {

                // Very serious case:
                // Debit succeeded,
                // target credit failed,
                // compensation also failed.

                payment.setStatus("COMPENSATION_FAILED");
                payment.setUpdatedAt(LocalDateTime.now());

                payment = paymentRepository.save(payment);

                saveOutboxEvent(payment, "COMPENSATION_FAILED");

                return payment;
            }
        }
    }

    public List<Payment> getAllPayments() {
        return paymentRepository.findAll();
    }

    public Payment getPaymentByReference(String paymentReference) {

        return paymentRepository
                .findByPaymentReference(paymentReference)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: "
                                        + paymentReference
                        )
                );
    }

    private void saveOutboxEvent(Payment payment, String eventType) {

        try {

            String payload = objectMapper.writeValueAsString(payment);

            OutboxEvent event = OutboxEvent.builder()
                    .eventId(UUID.randomUUID().toString())
                    .aggregateType("PAYMENT")
                    .aggregateId(payment.getPaymentReference())
                    .eventType(eventType)
                    .payload(payload)
                    .status("PENDING")
                    .createdAt(LocalDateTime.now())
                    .build();

            outboxEventRepository.save(event);

        } catch (JacksonException ex){

            throw new RuntimeException(
                    "Failed to create outbox event for payment: "
                            + payment.getPaymentReference(),
                    ex
            );
        }
    }
}