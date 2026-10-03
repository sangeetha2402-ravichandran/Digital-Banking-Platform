package com.digitalbanking.customer.controller;

import com.digitalbanking.customer.dto.CustomerRequest;
import com.digitalbanking.customer.entity.Customer;
import com.digitalbanking.customer.service.CustomerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/customers")
public class CustomerController {

    private final CustomerService customerService;

    public CustomerController(CustomerService customerService) {
        this.customerService = customerService;
    }

    @PostMapping
    public ResponseEntity<Customer> createCustomer(
            @Valid @RequestBody CustomerRequest request) {

        Customer customer = customerService.createCustomer(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(customer);
    }

    @GetMapping
    public ResponseEntity<List<Customer>> getAllCustomers() {
        return ResponseEntity.ok(customerService.getAllCustomers());
    }

    @GetMapping("/{id}")
    public ResponseEntity<Customer> getCustomerById(
            @PathVariable Long id) {

        return ResponseEntity.ok(customerService.getCustomerById(id));
    }

    @GetMapping("/number/{customerNumber}")
    public ResponseEntity<Customer> getCustomerByCustomerNumber(
            @PathVariable String customerNumber) {

        return ResponseEntity.ok(
                customerService.getCustomerByCustomerNumber(customerNumber)
        );
    }
}