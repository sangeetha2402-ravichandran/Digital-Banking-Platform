package com.digitalbanking.account.service;

import com.digitalbanking.account.client.CustomerClient;
import com.digitalbanking.account.dto.AccountRequest;
import com.digitalbanking.account.entity.Account;
import com.digitalbanking.account.entity.ProcessedAccountOperation;
import com.digitalbanking.account.exception.AccountNotFoundException;
import com.digitalbanking.account.exception.DuplicateAccountException;
import com.digitalbanking.account.repository.AccountRepository;
import com.digitalbanking.account.repository.ProcessedAccountOperationRepository;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Service
public class AccountService {

    private final AccountRepository accountRepository;
    private final CustomerClient customerClient;
    private final ProcessedAccountOperationRepository
            processedAccountOperationRepository;

    public AccountService(
            AccountRepository accountRepository,
            CustomerClient customerClient,
            ProcessedAccountOperationRepository
                    processedAccountOperationRepository) {

        this.accountRepository = accountRepository;
        this.customerClient = customerClient;
        this.processedAccountOperationRepository =
                processedAccountOperationRepository;
    }

    // =========================================================
    // CREATE ACCOUNT
    // =========================================================
    public Account createAccount(AccountRequest request) {

        if (!customerClient.customerExists(
                request.getCustomerId())) {

            throw new AccountNotFoundException(
                    "Customer not found with id: "
                            + request.getCustomerId()
            );
        }

        if (accountRepository
                .findByAccountNumber(
                        request.getAccountNumber())
                .isPresent()) {

            throw new DuplicateAccountException(
                    "Account number already exists: "
                            + request.getAccountNumber()
            );
        }

        Account account = Account.builder()
                .accountNumber(
                        request.getAccountNumber()
                )
                .customerId(
                        request.getCustomerId()
                )
                .accountType(
                        request.getAccountType()
                )
                .balance(
                        request.getBalance()
                )
                .currency(
                        request.getCurrency()
                )
                .status(
                        request.getStatus()
                )
                .build();

        return accountRepository.save(account);
    }

    // =========================================================
    // GET ALL ACCOUNTS
    // =========================================================
    public List<Account> getAllAccounts() {

        return accountRepository.findAll();
    }

    // =========================================================
    // GET ACCOUNT BY ID
    // =========================================================
    public Account getAccountById(Long id) {

        return accountRepository
                .findById(id)
                .orElseThrow(() ->
                        new AccountNotFoundException(
                                "Account not found with id: "
                                        + id
                        )
                );
    }

    // =========================================================
    // GET ACCOUNT BY ACCOUNT NUMBER
    // =========================================================
    public Account getAccountByAccountNumber(
            String accountNumber) {

        return accountRepository
                .findByAccountNumber(accountNumber)
                .orElseThrow(() ->
                        new AccountNotFoundException(
                                "Account not found with account number: "
                                        + accountNumber
                        )
                );
    }

    // =========================================================
    // GET ACCOUNTS BY CUSTOMER ID
    // =========================================================
    public List<Account> getAccountsByCustomerId(
            Long customerId) {

        return accountRepository
                .findByCustomerId(customerId);
    }

    // =========================================================
    // DEBIT ACCOUNT
    // =========================================================
    @Transactional
    public Account debitAccount(
            Long accountId,
            BigDecimal amount,
            String operationId) {

        Account account =
                getAccountById(accountId);

        // -----------------------------------------------------
        // 1. IDEMPOTENCY CHECK
        // -----------------------------------------------------
        if (processedAccountOperationRepository
                .existsById(operationId)) {

            System.out.println(
                    "Duplicate debit ignored: "
                            + operationId
            );

            return account;
        }

        // -----------------------------------------------------
        // 2. VALIDATE AMOUNT
        // -----------------------------------------------------
        if (amount == null ||
                amount.compareTo(BigDecimal.ZERO) <= 0) {

            throw new IllegalArgumentException(
                    "Debit amount must be greater than zero"
            );
        }

        // -----------------------------------------------------
        // 3. VALIDATE ACCOUNT STATUS
        // -----------------------------------------------------
        if (!"ACTIVE".equalsIgnoreCase(
                account.getStatus())) {

            throw new IllegalStateException(
                    "Account is not active: "
                            + accountId
            );
        }

        // -----------------------------------------------------
        // 4. CHECK BALANCE
        // -----------------------------------------------------
        if (account.getBalance()
                .compareTo(amount) < 0) {

            throw new IllegalStateException(
                    "Insufficient balance for account: "
                            + accountId
            );
        }

        // -----------------------------------------------------
        // 5. DEBIT BALANCE
        // -----------------------------------------------------
        BigDecimal newBalance =
                account.getBalance()
                        .subtract(amount);

        account.setBalance(newBalance);

        account =
                accountRepository.save(account);

        // -----------------------------------------------------
        // 6. SAVE PROCESSED OPERATION
        // -----------------------------------------------------
        ProcessedAccountOperation operation =
                ProcessedAccountOperation.builder()
                        .operationId(
                                operationId
                        )
                        .accountId(
                                accountId
                        )
                        .operationType(
                                "DEBIT"
                        )
                        .amount(
                                amount
                        )
                        .processedAt(
                                LocalDateTime.now()
                        )
                        .build();

        processedAccountOperationRepository
                .save(operation);

        System.out.println(
                "Debit completed: "
                        + operationId
        );

        return account;
    }

    // =========================================================
    // CREDIT ACCOUNT
    // =========================================================
    @Transactional
    public Account creditAccount(
            Long accountId,
            BigDecimal amount,
            String operationId) {

        Account account =
                getAccountById(accountId);

        // -----------------------------------------------------
        // 1. IDEMPOTENCY CHECK
        // -----------------------------------------------------
        if (processedAccountOperationRepository
                .existsById(operationId)) {

            System.out.println(
                    "Duplicate credit ignored: "
                            + operationId
            );

            return account;
        }

        // -----------------------------------------------------
        // 2. VALIDATE AMOUNT
        // -----------------------------------------------------
        if (amount == null ||
                amount.compareTo(BigDecimal.ZERO) <= 0) {

            throw new IllegalArgumentException(
                    "Credit amount must be greater than zero"
            );
        }

        // -----------------------------------------------------
        // 3. VALIDATE ACCOUNT STATUS
        // -----------------------------------------------------
        if (!"ACTIVE".equalsIgnoreCase(
                account.getStatus())) {

            throw new IllegalStateException(
                    "Account is not active: "
                            + accountId
            );
        }

        // -----------------------------------------------------
        // 4. CREDIT BALANCE
        // -----------------------------------------------------
        BigDecimal newBalance =
                account.getBalance()
                        .add(amount);

        account.setBalance(newBalance);

        account =
                accountRepository.save(account);

        // -----------------------------------------------------
        // 5. SAVE PROCESSED OPERATION
        // -----------------------------------------------------
        ProcessedAccountOperation operation =
                ProcessedAccountOperation.builder()
                        .operationId(
                                operationId
                        )
                        .accountId(
                                accountId
                        )
                        .operationType(
                                "CREDIT"
                        )
                        .amount(
                                amount
                        )
                        .processedAt(
                                LocalDateTime.now()
                        )
                        .build();

        processedAccountOperationRepository
                .save(operation);

        System.out.println(
                "Credit completed: "
                        + operationId
        );

        return account;
    }
}