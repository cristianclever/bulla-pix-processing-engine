package com.fintech.pix.domain.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.CONFLICT)
public class TransactionAlreadyExistsException extends RuntimeException {

    public TransactionAlreadyExistsException(String transactionId) {
        super("Transaction already exists or is currently being processed: " + transactionId);
    }
}