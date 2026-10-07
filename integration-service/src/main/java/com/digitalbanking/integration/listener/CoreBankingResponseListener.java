package com.digitalbanking.integration.listener;

import com.digitalbanking.integration.dto.CoreBankingResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
@RequiredArgsConstructor
public class CoreBankingResponseListener {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @JmsListener(destination = "DEV.QUEUE.2")
    public void receiveResponse(String message) {

        try {

            // 1. Convert MQ JSON response to Java object
            CoreBankingResponse response =
                    objectMapper.readValue(
                            message,
                            CoreBankingResponse.class
                    );

            System.out.println(
                    "Integration Service received Core Banking response: "
                            + response.getPaymentReference()
            );

            // 2. Decide Kafka topic based on Core Banking result
            String targetTopic;

            if ("SUCCESS".equalsIgnoreCase(response.getStatus())) {

                targetTopic = "core-processed";

            } else {

                targetTopic = "core-failed";
            }

            // 3. Convert response back to JSON for Kafka
            String payload =
                    objectMapper.writeValueAsString(response);

            // 4. Publish Kafka event
            kafkaTemplate.send(
                    targetTopic,
                    response.getPaymentId().toString(),
                    payload
            ).get();

            System.out.println(
                    "Core Banking result published to Kafka: "
                            + targetTopic
            );

        } catch (Exception e) {

            System.err.println(
                    "Failed to process Core Banking response: "
                            + e.getMessage()
            );
        }
    }
}