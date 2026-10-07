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
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class FundsTransferredConsumer {

    private final ObjectMapper objectMapper;
    private final LedgerTransactionRepository ledgerTransactionRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = "funds-transferred",
            groupId = "ledger-group"
    )
    public void consume(String message) {

        try {

            PaymentEvent event =
                    objectMapper.readValue(
                            message,
                            PaymentEvent.class
                    );

            System.out.println(
                    "Ledger Service received transferred payment: "
                            + event.getPaymentReference()
            );

            LedgerTransaction transaction =
                    LedgerTransaction.builder()
                            .paymentReference(
                                    event.getPaymentReference()
                            )
                            .paymentId(
                                    event.getPaymentId()
                            )
                            .sourceAccountNumber(
                                    event.getSourceAccountNumber()
                            )
                            .targetAccountNumber(
                                    event.getTargetAccountNumber()
                            )
                            .amount(
                                    event.getAmount()
                            )
                            .currency(
                                    event.getCurrency()
                            )
                            .status("POSTED")
                            .createdAt(
                                    LocalDateTime.now()
                            )
                            .build();

            ledgerTransactionRepository.save(transaction);

            // Ledger posting is a new event
            event.setEventId(
                    UUID.randomUUID().toString()
            );

            event.setEventType(
                    "LEDGER_UPDATED"
            );

            event.setStatus(
                    "POSTED"
            );

            String payload =
                    objectMapper.writeValueAsString(event);

            kafkaTemplate.send(
                    "ledger-updated",
                    event.getPaymentId().toString(),
                    payload
            ).get();

            System.out.println(
                    "Ledger posted payment: "
                            + event.getPaymentReference()
                            + " -> ledger-updated"
            );

        } catch (Exception e) {

            System.err.println(
                    "Ledger processing failed: "
                            + e.getMessage()
            );
        }
    }
}