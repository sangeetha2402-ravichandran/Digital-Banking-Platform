package com.digitalbanking.notification.consumer;

import com.digitalbanking.notification.dto.NotificationEvent;
import com.digitalbanking.notification.entity.Notification;
import com.digitalbanking.notification.repository.NotificationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.time.LocalDateTime;

@Component
@RequiredArgsConstructor
public class NotificationConsumer {

    private final ObjectMapper objectMapper;
    private final NotificationRepository notificationRepository;

    @KafkaListener(
            topics = "notification-events",
            groupId = "notification-group"
    )
    public void consumeFinalNotification(String message) {

        try {

            NotificationEvent event =
                    objectMapper.readValue(
                            message,
                            NotificationEvent.class
                    );

            String notificationMessage;

            if ("PAYMENT_COMPLETED".equals(event.getEventType())) {

                notificationMessage =
                        "Payment "
                                + event.getPaymentReference()
                                + " completed successfully for "
                                + event.getAmount()
                                + " "
                                + event.getCurrency();

            } else if ("PAYMENT_REVERSED".equals(event.getEventType())) {

                notificationMessage =
                        "Payment "
                                + event.getPaymentReference()
                                + " was reversed.";

            } else {

                notificationMessage =
                        "Payment update: "
                                + event.getStatus();
            }

            saveNotification(
                    event,
                    notificationMessage
            );

        } catch (Exception e) {

            System.err.println(
                    "Notification processing failed: "
                            + e.getMessage()
            );
        }
    }

    @KafkaListener(
            topics = "payment-rejected",
            groupId = "notification-rejected-group"
    )
    public void consumeRejectedPayment(String message) {

        try {

            NotificationEvent event =
                    objectMapper.readValue(
                            message,
                            NotificationEvent.class
                    );

            String notificationMessage =
                    "Payment "
                            + event.getPaymentReference()
                            + " was rejected by fraud screening.";

            saveNotification(
                    event,
                    notificationMessage
            );

        } catch (Exception e) {

            System.err.println(
                    "Rejected payment notification failed: "
                            + e.getMessage()
            );
        }
    }

    private void saveNotification(
            NotificationEvent event,
            String message) {

        Notification notification =
                Notification.builder()
                        .paymentId(event.getPaymentId())
                        .paymentReference(
                                event.getPaymentReference()
                        )
                        .eventType(
                                event.getEventType()
                        )
                        .status(
                                event.getStatus()
                        )
                        .message(message)
                        .createdAt(
                                LocalDateTime.now()
                        )
                        .build();

        notificationRepository.save(notification);

        System.out.println(
                "NOTIFICATION: " + message
        );
    }
}