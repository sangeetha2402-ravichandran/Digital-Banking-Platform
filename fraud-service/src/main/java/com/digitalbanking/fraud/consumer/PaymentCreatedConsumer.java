package com.digitalbanking.fraud.consumer;

import com.digitalbanking.fraud.dto.PaymentEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class PaymentCreatedConsumer {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @KafkaListener(
            topics = "payment-created",
            groupId = "fraud-group"
    )
    public void consume(String message) {

        try {

            PaymentEvent event =
                    objectMapper.readValue(message, PaymentEvent.class);

            System.out.println(
                    "Fraud Service received payment: "
                            + event.getPaymentReference()
            );

            String targetTopic;

            if (event.getAmount().doubleValue() > 10000) {

                event.setEventType("PAYMENT_REJECTED");
                event.setStatus("REJECTED");

                targetTopic = "payment-rejected";

            } else {

                event.setEventType("PAYMENT_APPROVED");
                event.setStatus("APPROVED");

                targetTopic = "payment-approved";
            }

            String payload =
                    objectMapper.writeValueAsString(event);

            kafkaTemplate.send(
                    targetTopic,
                    event.getPaymentId().toString(),
                    payload
            );

            System.out.println(
                    "Fraud result published to: " + targetTopic
            );

        } catch (Exception e) {

            System.err.println(
                    "Fraud processing failed: " + e.getMessage()
            );
        }
    }
}