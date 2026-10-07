package com.digitalbanking.payment.consumer;

import com.digitalbanking.payment.dto.PaymentEvent;
import com.digitalbanking.payment.entity.ProcessedEvent;
import com.digitalbanking.payment.repository.ProcessedEventRepository;
import com.digitalbanking.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class PaymentApprovedConsumer {

    private final ObjectMapper objectMapper;
    private final PaymentService paymentService;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(
            topics = "payment-approved",
            groupId = "payment-approved-group"
    )
    public void consume(String message) throws Exception {

        // 1. Convert Kafka JSON to PaymentEvent
        PaymentEvent event =
                objectMapper.readValue(
                        message,
                        PaymentEvent.class
                );

        System.out.println(
                "Payment Service received fraud approval: "
                        + event.getPaymentReference()
        );

        // 2. Validate eventId
        if (event.getEventId() == null ||
                event.getEventId().isBlank()) {

            throw new IllegalArgumentException(
                    "Kafka eventId is missing"
            );
        }

        // 3. CHECK WHETHER THIS EVENT WAS ALREADY PROCESSED
        if (processedEventRepository.existsById(
                event.getEventId())) {

            System.out.println(
                    "Duplicate Kafka event ignored: "
                            + event.getEventId()
            );

            return;
        }

        // 4. PROCESS PAYMENT
        paymentService.processApprovedPayment(
                event
        );

        // 5. MARK KAFKA EVENT AS PROCESSED
        ProcessedEvent processedEvent =
                ProcessedEvent.builder()
                        .eventId(
                                event.getEventId()
                        )
                        .eventType(
                                event.getEventType()
                        )
                        .processedAt(
                                LocalDateTime.now()
                        )
                        .build();

        processedEventRepository.save(
                processedEvent
        );

        System.out.println(
                "Kafka event processed successfully: "
                        + event.getEventId()
        );
    }
}