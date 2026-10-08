package com.wbmd.kafka2db.failure;

import com.wbmd.kafka2db.processing.KafkaEnvelope;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class FailureRecord {
    private final String failedAt;
    private final String sourceId;
    private final String topic;
    private final int partition;
    private final long offset;
    private final long kafkaTimestamp;
    private final String mapping;
    private final String stage;
    private final List<String> successfulTargets = new ArrayList<String>();
    private final List<String> failedTargets = new ArrayList<String>();
    private final Map<String, Integer> attempts = new LinkedHashMap<String, Integer>();
    private final Map<String, String> errors = new LinkedHashMap<String, String>();
    private String rawMessage;

    public FailureRecord(KafkaEnvelope envelope, String mapping, String stage) {
        this.failedAt = LocalDateTime.now().toString();
        this.sourceId = envelope.getSourceId();
        this.topic = envelope.getTopic();
        this.partition = envelope.getPartition();
        this.offset = envelope.getOffset();
        this.kafkaTimestamp = envelope.getKafkaTimestamp();
        this.mapping = mapping;
        this.stage = stage;
    }

    public void addSuccess(String target, int attemptCount) {
        successfulTargets.add(target);
        attempts.put(target, Integer.valueOf(attemptCount));
    }

    public void addFailure(String target, int attemptCount, Throwable error) {
        failedTargets.add(target);
        attempts.put(target, Integer.valueOf(attemptCount));
        errors.put(target, describe(error));
    }

    public void setRawMessage(String rawMessage) {
        this.rawMessage = rawMessage;
    }

    public String getFailedAt() {
        return failedAt;
    }

    public String getSourceId() {
        return sourceId;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public long getKafkaTimestamp() {
        return kafkaTimestamp;
    }

    public String getMapping() {
        return mapping;
    }

    public String getStage() {
        return stage;
    }

    public List<String> getSuccessfulTargets() {
        return successfulTargets;
    }

    public List<String> getFailedTargets() {
        return failedTargets;
    }

    public Map<String, Integer> getAttempts() {
        return attempts;
    }

    public Map<String, String> getErrors() {
        return errors;
    }

    public String getRawMessage() {
        return rawMessage;
    }

    private String describe(Throwable error) {
        if (error == null) {
            return "Unknown failure";
        }
        String message = error.getMessage();
        return error.getClass().getName() + (message == null ? "" : ": " + message);
    }
}

