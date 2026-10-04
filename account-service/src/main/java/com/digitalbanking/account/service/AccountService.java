package com.digitalbanking.account.service;

import com.digitalbanking.account.client.CustomerClient;
import com.digitalbanking.account.dto.AccountRequest;
import com.digitalbanking.account.entity.Account;
import com.digitalbanking.account.exception.AccountNotFoundException;
import com.digitalbanking.account.exception.DuplicateAccountException;
import com.digitalbanking.account.repository.AccountRepository;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;

@Service
public class AccountService {

    private final AccountRepository accountRepository;
    private final CustomerClient customerClient;

    public AccountService(AccountRepository accountRepository,
                          CustomerClient customerClient) {
        this.accountRepository = accountRepository;
        this.customerClient = customerClient;
    }

    public Account createAccount(AccountRequest request) {

        if (!customerClient.customerExists(request.getCustomerId())) {
            throw new AccountNotFoundException(
                    "Customer not found with id: " + request.getCustomerId()
            );
        }

        if (accountRepository.findByAccountNumber(request.getAccountNumber()).isPresent()) {
            throw new DuplicateAccountException(
                    "Account number already exists: " + request.getAccountNumber()
            );
        }

        Account account = Account.builder()
                .accountNumber(request.getAccountNumber())
                .customerId(request.getCustomerId())
                .accountType(request.getAccountType())
                .balance(request.getBalance())
                .currency(request.getCurrency())
                .status(request.getStatus())
                .build();

        return accountRepository.save(account);
    }

    public List<Account> getAllAccounts() {
        return accountRepository.findAll();
    }

    public Account getAccountById(Long id) {
        return accountRepository.findById(id)
                .orElseThrow(() ->
                        new AccountNotFoundException(
                                "Account not found with id: " + id
                        )
                );
    }

    public Account getAccountByAccountNumber(String accountNumber) {
        return accountRepository.findByAccountNumber(accountNumber)
                .orElseThrow(() ->
                        new AccountNotFoundException(
                                "Account not found with account number: " + accountNumber
                        )
                );
    }

    public List<Account> getAccountsByCustomerId(Long customerId) {
        return accountRepository.findByCustomerId(customerId);
    }

    @Transactional
    public Account debitAccount(Long accountId, BigDecimal amount) {

        Account account = getAccountById(accountId);

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(
                    "Debit amount must be greater than zero"
            );
        }

        if (!"ACTIVE".equalsIgnoreCase(account.getStatus())) {
            throw new IllegalStateException(
                    "Account is not active: " + accountId
            );
        }

        if (account.getBalance().compareTo(amount) < 0) {
            throw new IllegalStateException(
                    "Insufficient balance for account: " + accountId
            );
        }

        BigDecimal newBalance = account.getBalance().subtract(amount);

        account.setBalance(newBalance);

        return accountRepository.save(account);
    }

    @Transactional
    public Account creditAccount(Long accountId, BigDecimal amount) {

        Account account = getAccountById(accountId);

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(
                    "Credit amount must be greater than zero"
            );
        }

        if (!"ACTIVE".equalsIgnoreCase(account.getStatus())) {
            throw new IllegalStateException(
                    "Account is not active: " + accountId
            );
        }

        BigDecimal newBalance = account.getBalance().add(amount);

        account.setBalance(newBalance);

        return accountRepository.save(account);
    }
}