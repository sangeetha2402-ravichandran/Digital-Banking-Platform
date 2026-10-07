package com.digitalbanking.simulator.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoreBankingRequest {

    private String messageId;
    private String correlationId;

    private Long paymentId;
    private String paymentReference;

    private String sourceAccountNumber;
    private String targetAccountNumber;

    private BigDecimal amount;
    private String currency;

    private LocalDateTime requestTimestamp;
}