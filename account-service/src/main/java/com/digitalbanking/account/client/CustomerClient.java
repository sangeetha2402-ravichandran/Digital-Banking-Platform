package com.digitalbanking.account.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class CustomerClient {

    private final RestClient restClient;

    public CustomerClient(
            @Value("${customer.service.url}") String customerServiceUrl) {

        this.restClient = RestClient.builder()
                .baseUrl(customerServiceUrl)
                .build();
    }

    public boolean customerExists(Long customerId) {

        try {
            restClient.get()
                    .uri("/api/customers/{id}", customerId)
                    .retrieve()
                    .toBodilessEntity();

            return true;

        } catch (HttpClientErrorException.NotFound ex) {
            return false;
        }
    }
}