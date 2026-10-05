package com.digitalbanking.payment.dto;

import lombok.*;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentEvent {

    private String eventId;
    private String eventType;

    private Long paymentId;
    private String paymentReference;

    private String sourceAccountNumber;
    private String targetAccountNumber;

    private BigDecimal amount;
    private String currency;

    private String status;
}