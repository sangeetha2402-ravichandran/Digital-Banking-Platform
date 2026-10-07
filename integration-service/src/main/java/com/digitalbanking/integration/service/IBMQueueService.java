package com.digitalbanking.integration.service;

import lombok.RequiredArgsConstructor;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class IBMQueueService {

    private final JmsTemplate jmsTemplate;

    private static final String CORE_BANKING_REQUEST_QUEUE =
            "DEV.QUEUE.1";

    public void sendCoreBankingRequest(String payload) {

        jmsTemplate.convertAndSend(
                CORE_BANKING_REQUEST_QUEUE,
                payload
        );

        System.out.println(
                "Message sent to IBM MQ queue: "
                        + CORE_BANKING_REQUEST_QUEUE
        );
    }
}