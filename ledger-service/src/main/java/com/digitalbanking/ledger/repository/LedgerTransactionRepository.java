package com.digitalbanking.ledger.repository;

import com.digitalbanking.ledger.entity.LedgerTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LedgerTransactionRepository
        extends JpaRepository<LedgerTransaction, Long> {
}