package com.digitalbanking.fraud.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaTemplate<String, String> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer =
                new DeadLetterPublishingRecoverer(
                        kafkaTemplate,
                        (record, exception) ->
                                new TopicPartition(
                                        "payment-dlq",
                                        record.partition()
                                )
                );

        /*
         * 2000 ms = wait 2 seconds between retries
         * 2 retries = original attempt + 2 retries
         *           = 3 total attempts
         */
        FixedBackOff fixedBackOff =
                new FixedBackOff(
                        2000L,
                        2L
                );

        return new DefaultErrorHandler(
                recoverer,
                fixedBackOff
        );
    }
}