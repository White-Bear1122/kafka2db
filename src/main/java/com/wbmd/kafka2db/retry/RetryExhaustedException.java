package com.wbmd.kafka2db.retry;

import java.sql.SQLException;

public class RetryExhaustedException extends Exception {
    private final int attempts;

    public RetryExhaustedException(String operation, int attempts, SQLException cause) {
        super(operation + " failed after " + attempts + " attempts", cause);
        this.attempts = attempts;
    }

    public int getAttempts() {
        return attempts;
    }
}

