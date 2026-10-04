package com.digitalbanking.account.controller;

import com.digitalbanking.account.dto.AccountRequest;
import com.digitalbanking.account.dto.AmountRequest;
import com.digitalbanking.account.entity.Account;
import com.digitalbanking.account.service.AccountService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/accounts")
public class AccountController {

    private final AccountService accountService;

    public AccountController(AccountService accountService) {
        this.accountService = accountService;
    }

    @PostMapping
    public ResponseEntity<Account> createAccount(
            @Valid @RequestBody AccountRequest request) {

        Account account = accountService.createAccount(request);

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(account);
    }

    @GetMapping
    public ResponseEntity<List<Account>> getAllAccounts() {
        return ResponseEntity.ok(accountService.getAllAccounts());
    }

    @GetMapping("/{id}")
    public ResponseEntity<Account> getAccountById(
            @PathVariable Long id) {

        return ResponseEntity.ok(
                accountService.getAccountById(id)
        );
    }

    @GetMapping("/number/{accountNumber}")
    public ResponseEntity<Account> getAccountByAccountNumber(
            @PathVariable String accountNumber) {

        return ResponseEntity.ok(
                accountService.getAccountByAccountNumber(accountNumber)
        );
    }

    @GetMapping("/customer/{customerId}")
    public ResponseEntity<List<Account>> getAccountsByCustomerId(
            @PathVariable Long customerId) {

        return ResponseEntity.ok(
                accountService.getAccountsByCustomerId(customerId)
        );
    }



    @PutMapping("/{id}/debit")
    public ResponseEntity<Account> debitAccount(
            @PathVariable Long id,
            @Valid @RequestBody AmountRequest request) {

        Account account = accountService.debitAccount(
                id,
                request.getAmount()
        );

        return ResponseEntity.ok(account);
    }



    @PutMapping("/{id}/credit")
    public ResponseEntity<Account> creditAccount(
            @PathVariable Long id,
            @Valid @RequestBody AmountRequest request) {

        Account account = accountService.creditAccount(
                id,
                request.getAmount()
        );

        return ResponseEntity.ok(account);
    }

}