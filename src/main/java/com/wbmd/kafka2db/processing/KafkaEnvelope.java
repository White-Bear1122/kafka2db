package com.wbmd.kafka2db.processing;

import java.nio.charset.Charset;

public final class KafkaEnvelope {
    private final String sourceId;
    private final String topic;
    private final int partition;
    private final long offset;
    private final long kafkaTimestamp;
    private final byte[] payload;

    public KafkaEnvelope(String sourceId,
                         String topic,
                         int partition,
                         long offset,
                         long kafkaTimestamp,
                         byte[] payload) {
        this.sourceId = sourceId;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.kafkaTimestamp = kafkaTimestamp;
        this.payload = payload;
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

    public byte[] getPayload() {
        return payload;
    }

    public String rawMessage(String charsetName) {
        if (payload == null) {
            return null;
        }
        return new String(payload, Charset.forName(charsetName));
    }
}

