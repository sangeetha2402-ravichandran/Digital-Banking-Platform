package com.digitalbanking.payment.client;

import com.digitalbanking.payment.dto.AccountResponse;
import com.digitalbanking.payment.dto.AmountRequest;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;

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

    public AccountResponse getAccountByAccountNumber(String accountNumber) {

        return restClient.get()
                .uri("/api/accounts/number/{accountNumber}", accountNumber)
                .retrieve()
                .body(AccountResponse.class);
    }

    public AccountResponse debitAccount(
            Long accountId,
            BigDecimal amount) {

        AmountRequest request = new AmountRequest(amount);

        return restClient.put()
                .uri("/api/accounts/{id}/debit", accountId)
                .body(request)
                .retrieve()
                .body(AccountResponse.class);
    }

    public AccountResponse creditAccount(
            Long accountId,
            BigDecimal amount) {

        AmountRequest request = new AmountRequest(amount);

        return restClient.put()
                .uri("/api/accounts/{id}/credit", accountId)
                .body(request)
                .retrieve()
                .body(AccountResponse.class);
    }
}