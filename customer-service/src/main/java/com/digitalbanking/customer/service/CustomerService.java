package com.digitalbanking.customer.service;

import com.digitalbanking.customer.dto.CustomerRequest;
import com.digitalbanking.customer.entity.Customer;
import com.digitalbanking.customer.exception.CustomerNotFoundException;
import com.digitalbanking.customer.exception.DuplicateCustomerException;
import com.digitalbanking.customer.repository.CustomerRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CustomerService {

    private final CustomerRepository customerRepository;

    public CustomerService(CustomerRepository customerRepository) {
        this.customerRepository = customerRepository;
    }

    public Customer createCustomer(CustomerRequest request) {

        customerRepository.findByCustomerNumber(request.getCustomerNumber())
                .ifPresent(customer -> {
                    throw new DuplicateCustomerException(
                            "Customer number already exists: " + request.getCustomerNumber()
                    );
                });

        customerRepository.findByEmail(request.getEmail())
                .ifPresent(customer -> {
                    throw new DuplicateCustomerException(
                            "Email already exists: " + request.getEmail()
                    );
                });

        Customer customer = Customer.builder()
                .customerNumber(request.getCustomerNumber())
                .firstName(request.getFirstName())
                .lastName(request.getLastName())
                .email(request.getEmail())
                .phone(request.getPhone())
                .status(request.getStatus())
                .build();

        return customerRepository.save(customer);
    }

    public List<Customer> getAllCustomers() {
        return customerRepository.findAll();
    }

    public Customer getCustomerById(Long id) {
        return customerRepository.findById(id)
                .orElseThrow(() ->
                        new CustomerNotFoundException("Customer not found with id: " + id)
                );
    }

    public Customer getCustomerByCustomerNumber(String customerNumber) {
        return customerRepository.findByCustomerNumber(customerNumber)
                .orElseThrow(() ->
                        new CustomerNotFoundException(
                                "Customer not found with customer number: " + customerNumber
                        )
                );
    }

}