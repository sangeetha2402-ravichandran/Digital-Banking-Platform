package com.digitalbanking.payment.consumer;

import com.digitalbanking.payment.dto.PaymentEvent;
import com.digitalbanking.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class PaymentRejectedConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    @KafkaListener(
            topics = "payment-rejected",
            groupId = "payment-rejected-group"
    )
    public void consume(String message) {

        try {

            PaymentEvent event =
                    objectMapper.readValue(
                            message,
                            PaymentEvent.class
                    );

            System.out.println(
                    "Payment Service received Fraud REJECTION: "
                            + event.getPaymentReference()
            );

            paymentService.rejectPayment(
                    event.getPaymentId()
            );

        } catch (Exception e) {

            System.err.println(
                    "Failed to process payment rejection: "
                            + e.getMessage()
            );
        }
    }
}