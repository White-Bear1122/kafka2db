package com.wbmd.kafka2db.processing;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.failure.FailureRecord;
import com.wbmd.kafka2db.failure.FailureStore;
import com.wbmd.kafka2db.failure.FailureStoreException;
import com.wbmd.kafka2db.parser.KafkaMessageParser;
import com.wbmd.kafka2db.parser.MessageParseException;
import com.wbmd.kafka2db.parser.ParsedMessage;
import com.wbmd.kafka2db.writer.DatabaseWriteCoordinator;
import com.wbmd.kafka2db.writer.TargetWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public final class KafkaRecordProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordProcessor.class);

    private final AppConfig.RuntimeConfig runtime;
    private final KafkaMessageParser parser;
    private final DatabaseWriteCoordinator coordinator;
    private final FailureStore failureStore;

    public KafkaRecordProcessor(AppConfig.RuntimeConfig runtime,
                                KafkaMessageParser parser,
                                DatabaseWriteCoordinator coordinator,
                                FailureStore failureStore) {
        this.runtime = runtime;
        this.parser = parser;
        this.coordinator = coordinator;
        this.failureStore = failureStore;
    }

    public void process(KafkaEnvelope envelope,
                        String mappingName,
                        TopicMapping mapping) throws FailureStoreException {
        ParsedMessage message;
        try {
            message = parser.parse(envelope.getPayload(), mapping.getSource(), runtime);
        } catch (MessageParseException e) {
            recordParseFailure(envelope, mappingName, mapping, e);
            return;
        } catch (RuntimeException e) {
            recordParseFailure(envelope, mappingName, mapping, e);
            return;
        }

        List<TargetWriteResult> results = coordinator.write(mapping, message);
        boolean failed = false;
        for (TargetWriteResult result : results) {
            failed |= !result.isSuccess();
        }
        if (!failed) {
            LOGGER.info("Kafka message stored: source={}, topic={}, partition={}, offset={}, rows={}",
                    envelope.getSourceId(), envelope.getTopic(), envelope.getPartition(), envelope.getOffset(),
                    message.getRecords().size());
            return;
        }

        FailureRecord failure = new FailureRecord(envelope, mappingName, "database");
        for (TargetWriteResult result : results) {
            if (result.isSuccess()) {
                failure.addSuccess(result.getTarget().getConfigName(), result.getAttempts());
            } else {
                failure.addFailure(result.getTarget().getConfigName(), result.getAttempts(), result.getError());
            }
        }
        attachRawMessage(failure, envelope, mapping);
        failureStore.record(failure);
        LOGGER.error("Kafka message database failure was recorded: source={}, topic={}, partition={}, offset={}",
                envelope.getSourceId(), envelope.getTopic(), envelope.getPartition(), envelope.getOffset());
    }

    private void recordParseFailure(KafkaEnvelope envelope,
                                    String mappingName,
                                    TopicMapping mapping,
                                    Throwable error) throws FailureStoreException {
        FailureRecord failure = new FailureRecord(envelope, mappingName, "parse");
        failure.addFailure("parser", 1, error);
        attachRawMessage(failure, envelope, mapping);
        failureStore.record(failure);
        LOGGER.error("Kafka message parse failure was recorded: source={}, topic={}, partition={}, offset={}",
                envelope.getSourceId(), envelope.getTopic(), envelope.getPartition(), envelope.getOffset(), error);
    }

    private void attachRawMessage(FailureRecord failure,
                                  KafkaEnvelope envelope,
                                  TopicMapping mapping) {
        if (runtime.getFailure().isIncludeRawMessage()) {
            failure.setRawMessage(envelope.rawMessage(mapping.getSource().getCharset()));
        }
    }
}
