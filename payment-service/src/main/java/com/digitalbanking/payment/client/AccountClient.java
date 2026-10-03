package com.digitalbanking.payment.client;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class AccountClient {

    private final RestClient restClient;

    public AccountClient(
            @Value("${account.service.url}") String accountServiceUrl) {

        this.restClient = RestClient.builder()
                .baseUrl(accountServiceUrl)
                .build();
    }

    public boolean accountExists(String accountNumber) {

        try {
            restClient.get()
                    .uri("/api/accounts/number/{accountNumber}", accountNumber)
                    .retrieve()
                    .toBodilessEntity();

            return true;

        } catch (HttpClientErrorException.NotFound ex) {
            return false;
        }
    }
}