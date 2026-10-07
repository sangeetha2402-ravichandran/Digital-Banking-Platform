package com.digitalbanking.payment.consumer;

import com.digitalbanking.payment.dto.PaymentEvent;
import com.digitalbanking.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class PaymentApprovedConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    @KafkaListener(
            topics = "payment-approved",
            groupId = "payment-approved-group"
    )
    public void consume(String message) {

        try {

            PaymentEvent event =
                    objectMapper.readValue(
                            message,
                            PaymentEvent.class
                    );

            System.out.println(
                    "Payment Service received fraud approval: "
                            + event.getPaymentReference()
            );

            paymentService.processApprovedPayment(event);

        } catch (Exception e) {

            System.err.println(
                    "Approved payment processing failed: "
                            + e.getMessage()
            );
        }
    }
}