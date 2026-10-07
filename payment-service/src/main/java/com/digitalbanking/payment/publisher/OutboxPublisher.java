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

    private static final int MAX_RETRIES = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // =========================================================
    // POLL OUTBOX TABLE
    // =========================================================

    @Scheduled(fixedDelay = 5000)
    public void publishPendingEvents() {

        List<OutboxEvent> pendingEvents =
                outboxEventRepository.findByStatusOrderByCreatedAtAsc("PENDING");

        for (OutboxEvent event : pendingEvents) {

            publishEvent(event);
        }
    }


    // =========================================================
    // PUBLISH ONE EVENT
    // =========================================================

    private void publishEvent(OutboxEvent event) {

        try {

            String topic =
                    resolveTopic(event.getEventType());

            // Record the attempt time
            event.setLastAttemptAt(
                    LocalDateTime.now()
            );

            /*
             * KafkaTemplate.send() is asynchronous.
             *
             * .get() waits for Kafka acknowledgement.
             *
             * We mark the outbox event SENT only after
             * Kafka confirms that the message was published.
             */
            kafkaTemplate.send(
                    topic,
                    event.getAggregateId(),
                    event.getPayload()
            ).get();

            // =================================================
            // KAFKA CONFIRMED SUCCESS
            // =================================================

            event.setStatus("SENT");

            event.setSentAt(
                    LocalDateTime.now()
            );

            event.setLastError(null);

            outboxEventRepository.save(event);

            System.out.println(
                    "Outbox event published successfully. "
                            + "eventId="
                            + event.getEventId()
                            + ", eventType="
                            + event.getEventType()
                            + ", topic="
                            + topic
            );

        } catch (Exception exception) {

            handlePublishFailure(
                    event,
                    exception
            );
        }
    }


    // =========================================================
    // HANDLE KAFKA FAILURE
    // =========================================================

    private void handlePublishFailure(
            OutboxEvent event,
            Exception exception) {

        int currentRetryCount =
                event.getRetryCount() == null
                        ? 0
                        : event.getRetryCount();

        int newRetryCount =
                currentRetryCount + 1;

        event.setRetryCount(
                newRetryCount
        );

        event.setLastAttemptAt(
                LocalDateTime.now()
        );

        String errorMessage =
                exception.getMessage();

        if (errorMessage == null) {

            errorMessage =
                    exception
                            .getClass()
                            .getSimpleName();
        }

        event.setLastError(
                errorMessage
        );

        // =====================================================
        // MAX RETRIES REACHED
        // =====================================================

        if (newRetryCount >= MAX_RETRIES) {

            event.setStatus(
                    "FAILED"
            );

            System.out.println(
                    "Outbox event permanently FAILED. "
                            + "eventId="
                            + event.getEventId()
                            + ", retryCount="
                            + newRetryCount
                            + ", error="
                            + errorMessage
            );

        } else {

            /*
             * Keep PENDING.
             *
             * @Scheduled will pick this row again
             * on the next run.
             */
            event.setStatus(
                    "PENDING"
            );

            System.out.println(
                    "Outbox publish failed. Will retry. "
                            + "eventId="
                            + event.getEventId()
                            + ", retryCount="
                            + newRetryCount
                            + ", error="
                            + errorMessage
            );
        }

        outboxEventRepository.save(
                event
        );
    }


    // =========================================================
    // EVENT TYPE → KAFKA TOPIC
    // =========================================================

    private String resolveTopic(
            String eventType) {

        return switch (eventType) {

            case "PAYMENT_CREATED" ->
                    "payment-created";

            case "FUNDS_TRANSFERRED" ->
                    "funds-transferred";

            case "PAYMENT_COMPLETED" ->
                    "notification-events";

            case "PAYMENT_REVERSED" ->
                    "payment-reversed";

            case "REVERSAL_FAILED" ->
                    "payment-dlq";

            default ->
                    throw new IllegalArgumentException(
                            "No Kafka topic configured for event type: "
                                    + eventType
                    );
        };
    }
}