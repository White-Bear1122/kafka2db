package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.parser.ParsedMessage;
import com.wbmd.kafka2db.retry.RetryExecutor;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseWriteCoordinatorTest {
    @Test
    void retriesOnlyTheFailedDatabaseTarget() {
        AtomicInteger tdengineCalls = new AtomicInteger();
        AtomicInteger oceanbaseCalls = new AtomicInteger();
        TargetWriter tdengine = writer(TargetType.TDENGINE, tdengineCalls, 0);
        TargetWriter oceanbase = writer(TargetType.OCEANBASE, oceanbaseCalls, 3);
        AppConfig.RetryConfig retry = new AppConfig.RetryConfig();
        retry.setMaxRetries(3);
        retry.setBackoffMs(Arrays.asList(0L, 0L, 0L));
        DatabaseWriteCoordinator coordinator = new DatabaseWriteCoordinator(
                Arrays.asList(tdengine, oceanbase), new RetryExecutor(retry));

        TopicMapping mapping = new TopicMapping();
        mapping.setTdengine(new TopicMapping.TdengineMapping());
        mapping.setOceanbase(new TopicMapping.OceanBaseMapping());
        ParsedMessage message = new ParsedMessage(
                new LinkedHashMap<String, String>(), Collections.emptyList());

        List<TargetWriteResult> results = coordinator.write(mapping, message);

        assertEquals(1, tdengineCalls.get());
        assertEquals(4, oceanbaseCalls.get());
        assertEquals(2, results.size());
        assertTrue(results.get(0).isSuccess());
        assertTrue(results.get(1).isSuccess());
        assertEquals(4, results.get(1).getAttempts());
    }

    private TargetWriter writer(final TargetType type,
                                final AtomicInteger calls,
                                final int failuresBeforeSuccess) {
        return new TargetWriter() {
            @Override
            public TargetType getTargetType() {
                return type;
            }

            @Override
            public void write(TopicMapping mapping, ParsedMessage message) throws SQLException {
                if (calls.incrementAndGet() <= failuresBeforeSuccess) {
                    throw new SQLException(type + " unavailable");
                }
            }
        };
    }
}

