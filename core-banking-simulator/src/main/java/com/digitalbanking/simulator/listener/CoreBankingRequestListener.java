package com.digitalbanking.simulator.listener;

import com.digitalbanking.simulator.dto.CoreBankingRequest;
import com.digitalbanking.simulator.dto.CoreBankingResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class CoreBankingRequestListener {

    private final JmsTemplate jmsTemplate;
    private final ObjectMapper objectMapper;

    @JmsListener(destination = "DEV.QUEUE.1")
    public void processPayment(String message) {

        try {

 CoreBankingRequest request =
                    objectMapper.readValue(
                            message,
                            CoreBankingRequest.class
                    );

            System.out.println(
                    "Core Banking received payment: "
                            + request.getPaymentReference()
            );

            CoreBankingResponse response =
                    CoreBankingResponse.builder()
                            .messageId(
                                    UUID.randomUUID().toString()
                            )
                            .correlationId(
                                    request.getCorrelationId()
                            )
                            .paymentId(
                                    request.getPaymentId()
                            )
                            .paymentReference(
                                    request.getPaymentReference()
                            )
                            .status("SUCCESS")
                            .message(
                                    "Payment processed successfully"
                            )
                            .build();

            String responsePayload =
                    objectMapper.writeValueAsString(
                            response
                    );

            jmsTemplate.convertAndSend(
                    "DEV.QUEUE.2",
                    responsePayload
            );

            System.out.println(
                    "Core Banking response sent for payment: "
                            + request.getPaymentReference()
            );

        } catch (Exception e) {

            System.err.println(
                    "Core Banking processing failed: "
                            + e.getMessage()
            );
        }
    }
}