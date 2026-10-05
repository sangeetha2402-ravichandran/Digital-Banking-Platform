package com.digitalbanking.payment.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventConsumer {

    @KafkaListener(
            topics = "payment-events",
            groupId = "payment-group"
    )
    public void consume(String message) {

        System.out.println(
                "Received payment event: " + message
        );
    }
}