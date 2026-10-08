package com.wbmd.kafka2db.retry;

import com.wbmd.kafka2db.config.AppConfig;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryExecutorTest {
    @Test
    void initialAttemptPlusThreeRetriesCanSucceedOnFourthAttempt() throws Exception {
        RetryExecutor executor = executor();
        AtomicInteger calls = new AtomicInteger();

        int attempts = executor.execute("test-target", () -> {
            if (calls.incrementAndGet() < 4) {
                throw new SQLException("temporary failure");
            }
        });

        assertEquals(4, calls.get());
        assertEquals(4, attempts);
    }

    @Test
    void reportsFourAttemptsWhenAllAttemptsFail() {
        RetryExecutor executor = executor();
        AtomicInteger calls = new AtomicInteger();

        RetryExhaustedException error = assertThrows(RetryExhaustedException.class,
                () -> executor.execute("test-target", () -> {
                    calls.incrementAndGet();
                    throw new SQLException("still unavailable");
                }));

        assertEquals(4, calls.get());
        assertEquals(4, error.getAttempts());
    }

    private RetryExecutor executor() {
        AppConfig.RetryConfig config = new AppConfig.RetryConfig();
        config.setMaxRetries(3);
        config.setBackoffMs(Arrays.asList(0L, 0L, 0L));
        return new RetryExecutor(config);
    }
}
