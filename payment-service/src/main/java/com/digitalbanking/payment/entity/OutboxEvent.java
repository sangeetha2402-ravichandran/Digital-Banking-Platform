package com.digitalbanking.payment.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxEvent {

    @Id
    private String eventId;

    private String aggregateType;

    private String aggregateId;

    private String eventType;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private String status;

    // =========================================================
    // OUTBOX RETRY / FAILURE TRACKING
    // =========================================================

    private Integer retryCount;

    @Column(columnDefinition = "TEXT")
    private String lastError;

    private LocalDateTime lastAttemptAt;

    private LocalDateTime sentAt;

    private LocalDateTime createdAt;
}