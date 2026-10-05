package com.digitalbanking.payment.publisher;

import com.digitalbanking.payment.entity.OutboxEvent;
import com.digitalbanking.payment.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        List<OutboxEvent> events =
                outboxEventRepository.findByStatusOrderByCreatedAtAsc("PENDING");

        for (OutboxEvent event : events) {
            try {

                String topic;

                switch (event.getEventType()) {

                    case "PAYMENT_CREATED":
                        topic = "payment-created";
                        break;

                    case "PAYMENT_COMPLETED":
                        topic = "payment-approved";
                        break;

                    case "PAYMENT_FAILED":
                    case "PAYMENT_COMPENSATED":
                    case "COMPENSATION_FAILED":
                        topic = "payment-rejected";
                        break;

                    default:
                        throw new IllegalArgumentException(
                                "Unknown event type: " + event.getEventType()
                        );
                }

                kafkaTemplate.send(
                        topic,
                        event.getAggregateId(),
                        event.getPayload()
                ).get();

                event.setStatus("PUBLISHED");
                event.setPublishedAt(LocalDateTime.now());
                outboxEventRepository.save(event);

                System.out.println(
                        "Published outbox event: " + event.getId()
                );

            } catch (Exception e) {

                System.err.println(
                        "Failed to publish event: "
                                + event.getId()
                                + " - "
                                + e.getMessage()
                );
            }
        }
    }
    }
