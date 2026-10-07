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
public class PaymentReversedConsumer {

    private final ObjectMapper objectMapper;
    private final LedgerTransactionRepository ledgerTransactionRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = "payment-reversed",
            groupId = "ledger-reversal-group"
    )
    public void consume(String message) {

        try {

            // 1. Convert Kafka JSON to PaymentEvent
            PaymentEvent event =
                    objectMapper.readValue(
                            message,
                            PaymentEvent.class
                    );

            System.out.println(
                    "Ledger received payment reversal: "
                            + event.getPaymentReference()
            );

            // 2. Create reversal ledger entry
            LedgerTransaction reversal =
                    LedgerTransaction.builder()
                            .paymentId(event.getPaymentId())
                            .paymentReference(event.getPaymentReference())

                            // Reverse original direction
                            .sourceAccountNumber(
                                    event.getTargetAccountNumber()
                            )
                            .targetAccountNumber(
                                    event.getSourceAccountNumber()
                            )

                            .amount(event.getAmount())
                            .currency(event.getCurrency())
                            .status("REVERSED")
                            .createdAt(LocalDateTime.now())
                            .build();

            // 3. Save reversal audit record
            ledgerTransactionRepository.save(reversal);

            // 4. Prepare final notification event
            event.setEventId(
                    UUID.randomUUID().toString()
            );

            event.setEventType(
                    "PAYMENT_REVERSED"
            );

            event.setStatus(
                    "REVERSED"
            );

            String payload =
                    objectMapper.writeValueAsString(event);

            // 5. Send final failure/reversal notification event
            kafkaTemplate.send(
                    "notification-events",
                    event.getPaymentId().toString(),
                    payload
            ).get();

            System.out.println(
                    "Ledger reversal recorded: "
                            + event.getPaymentReference()
            );

        } catch (Exception e) {

            System.err.println(
                    "Ledger reversal processing failed: "
                            + e.getMessage()
            );
        }
    }
}