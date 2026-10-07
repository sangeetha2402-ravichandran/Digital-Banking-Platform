package com.digitalbanking.payment.service;

import com.digitalbanking.payment.client.AccountClient;
import com.digitalbanking.payment.dto.PaymentEvent;
import com.digitalbanking.payment.dto.PaymentRequest;
import com.digitalbanking.payment.entity.OutboxEvent;
import com.digitalbanking.payment.entity.Payment;
import com.digitalbanking.payment.exception.AccountNotFoundException;
import com.digitalbanking.payment.exception.PaymentNotFoundException;
import com.digitalbanking.payment.repository.OutboxEventRepository;
import com.digitalbanking.payment.repository.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import com.digitalbanking.payment.dto.AccountResponse;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final AccountClient accountClient;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public PaymentService(
            PaymentRepository paymentRepository,
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
        if (!accountClient.accountExists(
                request.getSourceAccountNumber())) {

            throw new AccountNotFoundException(
                    "Source account not found: "
                            + request.getSourceAccountNumber()
            );
        }

        // 3. VALIDATE TARGET ACCOUNT
        if (!accountClient.accountExists(
                request.getTargetAccountNumber())) {

            throw new AccountNotFoundException(
                    "Target account not found: "
                            + request.getTargetAccountNumber()
            );
        }

        LocalDateTime now = LocalDateTime.now();

        // 4. CREATE PAYMENT AS PENDING
        Payment payment = Payment.builder()
                .paymentReference(
                        "PAY-" + UUID.randomUUID()
                )
                .sourceAccountNumber(
                        request.getSourceAccountNumber()
                )
                .targetAccountNumber(
                        request.getTargetAccountNumber()
                )
                .amount(
                        request.getAmount()
                )
                .currency(
                        request.getCurrency()
                )
                .paymentType(
                        request.getPaymentType()
                )
                .status("PENDING")
                .idempotencyKey(
                        request.getIdempotencyKey()
                )
                .createdAt(now)
                .updatedAt(now)
                .build();

        // 5. SAVE PAYMENT
        payment = paymentRepository.save(payment);

        // 6. SAVE PAYMENT_CREATED EVENT TO OUTBOX
        saveOutboxEvent(
                payment,
                "PAYMENT_CREATED"
        );

        // 7. RETURN PENDING PAYMENT
        // Fraud Service will decide APPROVED / REJECTED
        return payment;
    }



    @Transactional
    public Payment processApprovedPayment(PaymentEvent event) {

        // 1. FIND ORIGINAL PAYMENT
        Payment payment = paymentRepository.findById(event.getPaymentId())
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: " + event.getPaymentId()
                        )
                );

        // 2. IDEMPOTENCY / DUPLICATE EVENT PROTECTION
        if ("FUNDS_TRANSFERRED".equals(payment.getStatus())) {
            return payment;
        }

        if ("COMPENSATED".equals(payment.getStatus())) {
            return payment;
        }

        if ("COMPENSATION_FAILED".equals(payment.getStatus())) {
            return payment;
        }

        // 3. GET ACCOUNT DETAILS
        AccountResponse sourceAccount =
                accountClient.getAccountByAccountNumber(
                        payment.getSourceAccountNumber()
                );

        AccountResponse targetAccount =
                accountClient.getAccountByAccountNumber(
                        payment.getTargetAccountNumber()
                );

        // 4. MARK PAYMENT APPROVED
        payment.setStatus("APPROVED");
        payment.setUpdatedAt(LocalDateTime.now());
        paymentRepository.save(payment);

        // 5. DEBIT SOURCE ACCOUNT
        try {

            accountClient.debitAccount(
                    sourceAccount.getId(),
                    payment.getAmount()
            );

        } catch (Exception debitException) {

            payment.setStatus("FAILED");
            payment.setUpdatedAt(LocalDateTime.now());

            paymentRepository.save(payment);

            saveOutboxEvent(
                    payment,
                    "PAYMENT_FAILED"
            );

            return payment;
        }

        // 6. CREDIT TARGET ACCOUNT
        try {

            accountClient.creditAccount(
                    targetAccount.getId(),
                    payment.getAmount()
            );

            // Do NOT mark COMPLETED yet.
            // Core Banking has not processed the transaction yet.
            payment.setStatus("FUNDS_TRANSFERRED");
            payment.setUpdatedAt(LocalDateTime.now());

            payment = paymentRepository.save(payment);

            saveOutboxEvent(
                    payment,
                    "FUNDS_TRANSFERRED"
            );

            return payment;

        } catch (Exception creditException) {

            // 7. SAGA COMPENSATION
            // Source debit succeeded but target credit failed.
            try {

                accountClient.creditAccount(
                        sourceAccount.getId(),
                        payment.getAmount()
                );

                payment.setStatus("COMPENSATED");
                payment.setUpdatedAt(LocalDateTime.now());

                payment = paymentRepository.save(payment);

                saveOutboxEvent(
                        payment,
                        "PAYMENT_COMPENSATED"
                );

                return payment;

            } catch (Exception compensationException) {

                payment.setStatus("COMPENSATION_FAILED");
                payment.setUpdatedAt(LocalDateTime.now());

                payment = paymentRepository.save(payment);

                saveOutboxEvent(
                        payment,
                        "COMPENSATION_FAILED"
                );

                return payment;
            }
        }
    }

    public List<Payment> getAllPayments() {

        return paymentRepository.findAll();
    }

    public Payment getPaymentByReference(
            String paymentReference) {

        return paymentRepository
                .findByPaymentReference(paymentReference)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: "
                                        + paymentReference
                        )
                );
    }

    private void saveOutboxEvent(
            Payment payment,
            String eventType) {

        try {

            PaymentEvent paymentEvent =
                    PaymentEvent.builder()
                            .eventId(
                                    UUID.randomUUID().toString()
                            )
                            .eventType(
                                    eventType
                            )
                            .paymentId(
                                    payment.getId()
                            )
                            .paymentReference(
                                    payment.getPaymentReference()
                            )
                            .sourceAccountNumber(
                                    payment.getSourceAccountNumber()
                            )
                            .targetAccountNumber(
                                    payment.getTargetAccountNumber()
                            )
                            .amount(
                                    payment.getAmount()
                            )
                            .currency(
                                    payment.getCurrency()
                            )
                            .status(
                                    payment.getStatus()
                            )
                            .build();

            OutboxEvent outboxEvent =
                    OutboxEvent.builder()
                            .eventId(
                                    paymentEvent.getEventId()
                            )
                            .aggregateType("PAYMENT")
                            .aggregateId(
                                    payment.getId().toString()
                            )
                            .eventType(
                                    paymentEvent.getEventType()
                            )
                            .payload(
                                    objectMapper.writeValueAsString(
                                            paymentEvent
                                    )
                            )
                            .status("PENDING")
                            .createdAt(
                                    LocalDateTime.now()
                            )
                            .build();

            outboxEventRepository.save(
                    outboxEvent
            );

        } catch (JacksonException ex) {

            throw new RuntimeException(
                    "Failed to create outbox event for payment: "
                            + payment.getPaymentReference(),
                    ex
            );
        }
    }



    @Transactional
    public Payment completePayment(Long paymentId) {

        Payment payment = paymentRepository
                .findById(paymentId)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: " + paymentId
                        )
                );

        // Duplicate Kafka event protection
        if ("COMPLETED".equals(payment.getStatus())) {
            return payment;
        }

        payment.setStatus("COMPLETED");
        payment.setUpdatedAt(LocalDateTime.now());

        payment = paymentRepository.save(payment);

        // Create final notification event
        saveOutboxEvent(
                payment,
                "PAYMENT_COMPLETED"
        );

        System.out.println(
                "Payment completed: "
                        + payment.getPaymentReference()
        );

        return payment;
    }

    @Transactional
    public Payment reversePayment(Long paymentId) {

        Payment payment = paymentRepository
                .findById(paymentId)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: " + paymentId
                        )
                );

        // Duplicate event protection
        if ("REVERSED".equals(payment.getStatus())) {
            return payment;
        }

        AccountResponse sourceAccount =
                accountClient.getAccountByAccountNumber(
                        payment.getSourceAccountNumber()
                );

        AccountResponse targetAccount =
                accountClient.getAccountByAccountNumber(
                        payment.getTargetAccountNumber()
                );

        try {

            // Reverse original target credit
            accountClient.debitAccount(
                    targetAccount.getId(),
                    payment.getAmount()
            );

            // Return money to original source
            accountClient.creditAccount(
                    sourceAccount.getId(),
                    payment.getAmount()
            );

            payment.setStatus("REVERSED");
            payment.setUpdatedAt(LocalDateTime.now());

            payment = paymentRepository.save(payment);

            saveOutboxEvent(
                    payment,
                    "PAYMENT_REVERSED"
            );

            System.out.println(
                    "Payment reversed successfully: "
                            + payment.getPaymentReference()
            );

            return payment;

        } catch (Exception e) {

            payment.setStatus("REVERSAL_FAILED");
            payment.setUpdatedAt(LocalDateTime.now());

            payment = paymentRepository.save(payment);

            saveOutboxEvent(
                    payment,
                    "REVERSAL_FAILED"
            );

            return payment;
        }
    }

    @Transactional
    public Payment rejectPayment(Long paymentId) {

        Payment payment = paymentRepository
                .findById(paymentId)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: " + paymentId
                        )
                );

        // Duplicate Kafka event protection
        if ("REJECTED".equals(payment.getStatus())) {
            return payment;
        }

        payment.setStatus("REJECTED");
        payment.setUpdatedAt(LocalDateTime.now());

        payment = paymentRepository.save(payment);

        System.out.println(
                "Payment rejected by Fraud Service: "
                        + payment.getPaymentReference()
        );

        return payment;
    }
}