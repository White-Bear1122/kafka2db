package com.wbmd.kafka2db.retry;

import com.wbmd.kafka2db.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

public final class RetryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetryExecutor.class);

    private final int maxRetries;
    private final List<Long> backoffMs;
    private final Sleeper sleeper;

    public RetryExecutor(AppConfig.RetryConfig config) {
        this(config.getMaxRetries(), config.getBackoffMs(), new ThreadSleeper());
    }

    RetryExecutor(int maxRetries, List<Long> backoffMs, Sleeper sleeper) {
        this.maxRetries = maxRetries;
        this.backoffMs = backoffMs;
        this.sleeper = sleeper;
    }

    public int execute(String operationName, SqlOperation operation) throws RetryExhaustedException {
        int totalAttempts = maxRetries + 1;
        SQLException last = null;
        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                operation.run();
                return attempt;
            } catch (SQLException e) {
                last = e;
                if (attempt >= totalAttempts) {
                    break;
                }
                long delay = backoffFor(attempt - 1);
                LOGGER.warn("{} failed on attempt {}/{}, retrying in {} ms: {}",
                        operationName, attempt, totalAttempts, delay, e.getMessage());
                try {
                    sleeper.sleep(delay);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    SQLException stopped = new SQLException("Retry interrupted", interrupted);
                    stopped.addSuppressed(e);
                    throw new RetryExhaustedException(operationName, attempt, stopped);
                }
            }
        }
        throw new RetryExhaustedException(operationName, totalAttempts, last);
    }

    private long backoffFor(int retryIndex) {
        int index = Math.min(retryIndex, backoffMs.size() - 1);
        return backoffMs.get(index).longValue();
    }

    public interface SqlOperation {
        void run() throws SQLException;
    }

    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private static final class ThreadSleeper implements Sleeper {
        @Override
        public void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
        }
    }
}

