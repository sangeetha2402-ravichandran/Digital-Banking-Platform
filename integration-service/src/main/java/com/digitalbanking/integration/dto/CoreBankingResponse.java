package com.digitalbanking.integration.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CoreBankingResponse {

    private String messageId;
    private String correlationId;

    private Long paymentId;
    private String paymentReference;

    private String status;
    private String message;
}