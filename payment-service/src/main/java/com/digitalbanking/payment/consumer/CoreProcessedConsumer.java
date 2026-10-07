package com.digitalbanking.payment.consumer;

import com.digitalbanking.payment.dto.CoreBankingResponse;
import com.digitalbanking.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class CoreProcessedConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    @KafkaListener(
            topics = "core-processed",
            groupId = "payment-core-group"
    )
    public void consume(String message) {

        try {

            CoreBankingResponse response =
                    objectMapper.readValue(
                            message,
                            CoreBankingResponse.class
                    );

            System.out.println(
                    "Payment Service received Core Banking SUCCESS: "
                            + response.getPaymentReference()
            );

            paymentService.completePayment(
                    response.getPaymentId()
            );

        } catch (Exception e) {

            System.err.println(
                    "Failed to process core-processed event: "
                            + e.getMessage()
            );
        }
    }
}