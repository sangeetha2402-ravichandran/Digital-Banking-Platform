package com.digitalbanking.payment.controller;

import com.digitalbanking.payment.dto.PaymentRequest;
import com.digitalbanking.payment.entity.Payment;
import com.digitalbanking.payment.service.PaymentService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PostMapping
    public ResponseEntity<Payment> createPayment(
            @Valid @RequestBody PaymentRequest request) {

        Payment payment = paymentService.createPayment(request);

        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(payment);
    }

    @GetMapping
    public ResponseEntity<List<Payment>> getAllPayments() {
        return ResponseEntity.ok(
                paymentService.getAllPayments()
        );
    }

    @GetMapping("/reference/{paymentReference}")
    public ResponseEntity<Payment> getPaymentByReference(
            @PathVariable String paymentReference) {

        return ResponseEntity.ok(
                paymentService.getPaymentByReference(paymentReference)
        );
    }
}