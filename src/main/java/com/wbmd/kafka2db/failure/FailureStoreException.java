package com.wbmd.kafka2db.failure;

public class FailureStoreException extends Exception {
    public FailureStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}

