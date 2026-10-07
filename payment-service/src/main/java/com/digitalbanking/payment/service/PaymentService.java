package com.digitalbanking.payment.service;

import com.digitalbanking.payment.client.AccountClient;
import com.digitalbanking.payment.dto.AccountResponse;
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

    // =========================================================
    // CREATE PAYMENT
    // =========================================================

    @Transactional
    public Payment createPayment(PaymentRequest request) {

        // 1. IDEMPOTENCY CHECK
        Payment existingPayment = paymentRepository
                .findByIdempotencyKey(request.getIdempotencyKey())
                .orElse(null);

        if (existingPayment != null) {

            System.out.println(
                    "Duplicate payment request ignored. Existing payment: "
                            + existingPayment.getPaymentReference()
            );

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

        System.out.println(
                "Payment created: "
                        + payment.getPaymentReference()
        );

        // 7. RETURN PENDING PAYMENT
        // Fraud Service will decide APPROVED / REJECTED
        return payment;
    }


    // =========================================================
    // PROCESS FRAUD-APPROVED PAYMENT
    // =========================================================

    @Transactional
    public Payment processApprovedPayment(PaymentEvent event) {

        // 1. FIND ORIGINAL PAYMENT
        Payment payment = paymentRepository
                .findById(event.getPaymentId())
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: "
                                        + event.getPaymentId()
                        )
                );

        // =====================================================
        // 2. PAYMENT-LEVEL IDEMPOTENCY
        // =====================================================
        // Do not move money again if the payment already
        // passed this processing stage.

        if ("FUNDS_TRANSFERRED".equals(payment.getStatus())
                || "COMPLETED".equals(payment.getStatus())
                || "REVERSED".equals(payment.getStatus())
                || "COMPENSATED".equals(payment.getStatus())
                || "COMPENSATION_FAILED".equals(payment.getStatus())
                || "REJECTED".equals(payment.getStatus())) {

            System.out.println(
                    "Payment already processed. Skipping duplicate: "
                            + payment.getPaymentReference()
                            + " status="
                            + payment.getStatus()
            );

            return payment;
        }

        // =====================================================
        // 3. VALID STATUS CHECK
        // =====================================================

        if (!"PENDING".equals(payment.getStatus())
                && !"APPROVED".equals(payment.getStatus())) {

            throw new IllegalStateException(
                    "Payment cannot be processed from status: "
                            + payment.getStatus()
            );
        }

        // =====================================================
        // 4. GET ACCOUNT DETAILS
        // =====================================================

        AccountResponse sourceAccount =
                accountClient.getAccountByAccountNumber(
                        payment.getSourceAccountNumber()
                );

        AccountResponse targetAccount =
                accountClient.getAccountByAccountNumber(
                        payment.getTargetAccountNumber()
                );

        // =====================================================
        // 5. CREATE IDEMPOTENT ACCOUNT OPERATION IDS
        // =====================================================

        String debitOperationId =
                "PAYMENT-" + payment.getId()
                        + "-DEBIT";

        String creditOperationId =
                "PAYMENT-" + payment.getId()
                        + "-CREDIT";

        String compensationOperationId =
                "PAYMENT-" + payment.getId()
                        + "-COMPENSATION";

        // =====================================================
        // 6. MARK FRAUD APPROVAL
        // =====================================================

        payment.setStatus("APPROVED");
        payment.setUpdatedAt(LocalDateTime.now());

        paymentRepository.save(payment);

        // =====================================================
        // 7. DEBIT SOURCE ACCOUNT
        // =====================================================

        try {

            accountClient.debitAccount(
                    sourceAccount.getId(),
                    payment.getAmount(),
                    debitOperationId
            );

            System.out.println(
                    "Source account debited for payment: "
                            + payment.getPaymentReference()
            );

        } catch (Exception debitException) {

            payment.setStatus("FAILED");
            payment.setUpdatedAt(LocalDateTime.now());

            payment = paymentRepository.save(payment);

            saveOutboxEvent(
                    payment,
                    "PAYMENT_FAILED"
            );

            System.out.println(
                    "Debit failed for payment: "
                            + payment.getPaymentReference()
            );

            return payment;
        }

        // =====================================================
        // 8. CREDIT TARGET ACCOUNT
        // =====================================================

        try {

            accountClient.creditAccount(
                    targetAccount.getId(),
                    payment.getAmount(),
                    creditOperationId
            );

            payment.setStatus("FUNDS_TRANSFERRED");
            payment.setUpdatedAt(LocalDateTime.now());

            payment = paymentRepository.save(payment);

            saveOutboxEvent(
                    payment,
                    "FUNDS_TRANSFERRED"
            );

            System.out.println(
                    "Funds transferred successfully: "
                            + payment.getPaymentReference()
            );

            return payment;

        } catch (Exception creditException) {

            // =================================================
            // 9. SAGA COMPENSATION
            // =================================================
            // Debit succeeded but credit failed.
            // Return money to the original source account.

            try {

                accountClient.creditAccount(
                        sourceAccount.getId(),
                        payment.getAmount(),
                        compensationOperationId
                );

                payment.setStatus("COMPENSATED");
                payment.setUpdatedAt(LocalDateTime.now());

                payment = paymentRepository.save(payment);

                saveOutboxEvent(
                        payment,
                        "PAYMENT_COMPENSATED"
                );

                System.out.println(
                        "Payment compensated successfully: "
                                + payment.getPaymentReference()
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

                System.out.println(
                        "Payment compensation failed: "
                                + payment.getPaymentReference()
                );

                return payment;
            }
        }
    }


    // =========================================================
    // GET ALL PAYMENTS
    // =========================================================

    public List<Payment> getAllPayments() {

        return paymentRepository.findAll();
    }


    // =========================================================
    // GET PAYMENT BY REFERENCE
    // =========================================================

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


    // =========================================================
    // SAVE OUTBOX EVENT
    // =========================================================

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
                            .aggregateType(
                                    "PAYMENT"
                            )
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
                            .status(
                                    "PENDING"
                            )
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


    // =========================================================
    // COMPLETE PAYMENT
    // =========================================================

    @Transactional
    public Payment completePayment(Long paymentId) {

        Payment payment = paymentRepository
                .findById(paymentId)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: "
                                        + paymentId
                        )
                );

        // Duplicate Kafka event protection
        if ("COMPLETED".equals(payment.getStatus())) {

            System.out.println(
                    "Payment already completed: "
                            + payment.getPaymentReference()
            );

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


    // =========================================================
    // REVERSE PAYMENT
    // =========================================================

    @Transactional
    public Payment reversePayment(Long paymentId) {

        Payment payment = paymentRepository
                .findById(paymentId)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: "
                                        + paymentId
                        )
                );

        // =====================================================
        // DUPLICATE REVERSAL PROTECTION
        // =====================================================

        if ("REVERSED".equals(payment.getStatus())) {

            System.out.println(
                    "Payment already reversed: "
                            + payment.getPaymentReference()
            );

            return payment;
        }

        // =====================================================
        // GET ACCOUNT DETAILS
        // =====================================================

        AccountResponse sourceAccount =
                accountClient.getAccountByAccountNumber(
                        payment.getSourceAccountNumber()
                );

        AccountResponse targetAccount =
                accountClient.getAccountByAccountNumber(
                        payment.getTargetAccountNumber()
                );

        // =====================================================
        // IDEMPOTENT REVERSAL OPERATION IDS
        // =====================================================

        String reversalDebitOperationId =
                "PAYMENT-" + payment.getId()
                        + "-REVERSAL-DEBIT-TARGET";

        String reversalCreditOperationId =
                "PAYMENT-" + payment.getId()
                        + "-REVERSAL-CREDIT-SOURCE";

        try {

            // =================================================
            // 1. REMOVE MONEY FROM ORIGINAL TARGET
            // =================================================

            accountClient.debitAccount(
                    targetAccount.getId(),
                    payment.getAmount(),
                    reversalDebitOperationId
            );

            // =================================================
            // 2. RETURN MONEY TO ORIGINAL SOURCE
            // =================================================

            accountClient.creditAccount(
                    sourceAccount.getId(),
                    payment.getAmount(),
                    reversalCreditOperationId
            );

            // =================================================
            // 3. MARK PAYMENT REVERSED
            // =================================================

            payment.setStatus("REVERSED");
            payment.setUpdatedAt(LocalDateTime.now());

            payment = paymentRepository.save(payment);

            // =================================================
            // 4. CREATE REVERSAL OUTBOX EVENT
            // =================================================

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

            System.out.println(
                    "Payment reversal failed: "
                            + payment.getPaymentReference()
            );

            return payment;
        }
    }


    // =========================================================
    // REJECT PAYMENT
    // =========================================================

    @Transactional
    public Payment rejectPayment(Long paymentId) {

        Payment payment = paymentRepository
                .findById(paymentId)
                .orElseThrow(() ->
                        new PaymentNotFoundException(
                                "Payment not found: "
                                        + paymentId
                        )
                );

        // Duplicate Kafka event protection
        if ("REJECTED".equals(payment.getStatus())) {

            System.out.println(
                    "Payment already rejected: "
                            + payment.getPaymentReference()
            );

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