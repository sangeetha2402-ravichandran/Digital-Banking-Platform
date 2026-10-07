package com.digitalbanking.account.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "processed_account_operations")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProcessedAccountOperation {

    @Id
    private String operationId;

    private Long accountId;

    private String operationType;

    private BigDecimal amount;

    private LocalDateTime processedAt;
}