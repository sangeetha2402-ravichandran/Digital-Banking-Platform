package com.digitalbanking.ledger.consumer;

import com.digitalbanking.ledger.dto.PaymentEvent;
import com.digitalbanking.ledger.entity.LedgerTransaction;
import com.digitalbanking.ledger.repository.LedgerTransactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class PaymentApprovedConsumer {

    private final ObjectMapper objectMapper;
    private final LedgerTransactionRepository ledgerTransactionRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = "payment-approved",
            groupId = "ledger-group"
    )
    public void consume(String message) {

        try {

            PaymentEvent event =
                    objectMapper.readValue(message, PaymentEvent.class);

            LedgerTransaction transaction =
                    LedgerTransaction.builder()
                            .paymentReference(event.getPaymentReference())
                            .paymentId(event.getPaymentId())
                            .sourceAccountNumber(event.getSourceAccountNumber())
                            .targetAccountNumber(event.getTargetAccountNumber())
                            .amount(event.getAmount())
                            .currency(event.getCurrency())
                            .status("POSTED")
                            .createdAt(LocalDateTime.now())
                            .build();

            ledgerTransactionRepository.save(transaction);

            event.setEventType("PAYMENT_POSTED");
            event.setStatus("POSTED");

            String payload =
                    objectMapper.writeValueAsString(event);

            kafkaTemplate.send(
                    "notification-events",
                    event.getPaymentId().toString(),
                    payload
            );

            System.out.println(
                    "Ledger posted payment: "
                            + event.getPaymentReference()
            );

        } catch (Exception e) {

            System.err.println(
                    "Ledger processing failed: "
                            + e.getMessage()
            );
        }
    }
}