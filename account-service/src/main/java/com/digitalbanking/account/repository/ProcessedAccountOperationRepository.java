package com.digitalbanking.account.repository;

import com.digitalbanking.account.entity.ProcessedAccountOperation;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedAccountOperationRepository
        extends JpaRepository<ProcessedAccountOperation, String> {
}