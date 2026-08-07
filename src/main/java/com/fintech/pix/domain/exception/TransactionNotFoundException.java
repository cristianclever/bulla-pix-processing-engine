package com.fintech.pix.domain.exception;

public class TransactionNotFoundException extends RuntimeException {

    public TransactionNotFoundException(String transactionId) {
        super("Transaction not found with id: " + transactionId);
    }
}
