package com.digitalbanking.payment.consumer;

import com.digitalbanking.payment.dto.CoreBankingResponse;
import com.digitalbanking.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class CoreFailedConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;

    @KafkaListener(
            topics = "core-failed",
            groupId = "payment-core-failed-group"
    )
    public void consume(String message) {

        try {

            CoreBankingResponse response =
                    objectMapper.readValue(
                            message,
                            CoreBankingResponse.class
                    );

            System.out.println(
                    "Payment Service received Core Banking FAILURE: "
                            + response.getPaymentReference()
            );

            paymentService.reversePayment(
                    response.getPaymentId()
            );

        } catch (Exception e) {

            System.err.println(
                    "Core failure compensation failed: "
                            + e.getMessage()
            );
        }
    }
}