package com.digitalbanking.integration.consumer;

import com.digitalbanking.integration.dto.CoreBankingRequest;
import com.digitalbanking.integration.dto.PaymentEvent;
import com.digitalbanking.integration.service.IBMQueueService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class LedgerUpdatedConsumer {

    private final ObjectMapper objectMapper;
    private final IBMQueueService ibmQueueService;

    @KafkaListener(
            topics = "ledger-updated",
            groupId = "integration-group"
    )
    public void consume(String message) {

        try {

            // 1. Convert Kafka JSON into PaymentEvent
            PaymentEvent event =
                    objectMapper.readValue(
                            message,
                            PaymentEvent.class
                    );

            System.out.println(
                    "Integration Service received ledger event: "
                            + event.getPaymentReference()
            );

            // 2. Build message for Core Banking / IBM MQ
            CoreBankingRequest coreBankingRequest =
                    CoreBankingRequest.builder()
                            .messageId(
                                    UUID.randomUUID().toString()
                            )
                            .correlationId(
                                    event.getEventId()
                            )
                            .paymentId(
                                    event.getPaymentId()
                            )
                            .paymentReference(
                                    event.getPaymentReference()
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
                            .requestTimestamp(
                                    LocalDateTime.now()
                            )
                            .build();

            // 3. Convert request object to JSON
            String mqPayload =
                    objectMapper.writeValueAsString(
                            coreBankingRequest
                    );

            ibmQueueService.sendCoreBankingRequest(mqPayload);
            System.out.println(
                    "Core Banking request prepared:"
            );

            System.out.println(mqPayload);

            // NEXT:
            // Send mqPayload to IBM MQ

        } catch (Exception e) {

            System.err.println(
                    "Integration processing failed: "
                            + e.getMessage()
            );
        }
    }
}