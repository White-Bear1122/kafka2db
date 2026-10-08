package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.mapping.MappingException;
import com.wbmd.kafka2db.parser.ParsedMessage;
import com.wbmd.kafka2db.retry.RetryExhaustedException;
import com.wbmd.kafka2db.retry.RetryExecutor;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public final class DatabaseWriteCoordinator {
    private final Map<TargetType, TargetWriter> writers = new EnumMap<TargetType, TargetWriter>(TargetType.class);
    private final RetryExecutor retryExecutor;

    public DatabaseWriteCoordinator(List<TargetWriter> targetWriters, RetryExecutor retryExecutor) {
        for (TargetWriter writer : targetWriters) {
            writers.put(writer.getTargetType(), writer);
        }
        this.retryExecutor = retryExecutor;
    }

    public List<TargetWriteResult> write(TopicMapping mapping, ParsedMessage message) {
        List<TargetWriteResult> results = new ArrayList<TargetWriteResult>();
        if (mapping.getTdengine() != null) {
            results.add(writeTarget(TargetType.TDENGINE, mapping, message));
        }
        if (mapping.getOceanbase() != null) {
            results.add(writeTarget(TargetType.OCEANBASE, mapping, message));
        }
        return results;
    }

    private TargetWriteResult writeTarget(final TargetType target,
                                          final TopicMapping mapping,
                                          final ParsedMessage message) {
        final TargetWriter writer = writers.get(target);
        if (writer == null) {
            return TargetWriteResult.failure(target, 0,
                    new IllegalStateException("No writer configured for " + target.getConfigName()));
        }
        try {
            int attempts = retryExecutor.execute(target.getConfigName(), new RetryExecutor.SqlOperation() {
                @Override
                public void run() throws java.sql.SQLException {
                    writer.write(mapping, message);
                }
            });
            return TargetWriteResult.success(target, attempts);
        } catch (RetryExhaustedException e) {
            return TargetWriteResult.failure(target, e.getAttempts(), e.getCause());
        } catch (MappingException e) {
            return TargetWriteResult.failure(target, 1, e);
        } catch (RuntimeException e) {
            return TargetWriteResult.failure(target, 1, e);
        }
    }
}
