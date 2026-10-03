package com.digitalbanking.account.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class AccountRequest {

    @NotBlank
    private String accountNumber;

    @NotNull
    private Long customerId;

    @NotBlank
    private String accountType;

    @NotNull
    @DecimalMin(value = "0.00")
    private BigDecimal balance;

    @NotBlank
    private String currency;

    @NotBlank
    private String status;
}