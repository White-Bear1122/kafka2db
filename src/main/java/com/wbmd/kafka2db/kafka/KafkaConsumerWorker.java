package com.wbmd.kafka2db.kafka;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.failure.FailureStoreException;
import com.wbmd.kafka2db.processing.KafkaEnvelope;
import com.wbmd.kafka2db.processing.KafkaRecordProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

final class KafkaConsumerWorker implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerWorker.class);

    private final AppConfig.KafkaSourceConfig source;
    private final int workerIndex;
    private final AppConfig.RuntimeConfig runtime;
    private final Map<String, TopicMapping> allMappings;
    private final KafkaRecordProcessor processor;
    private final Map<String, String> topicMappings = new LinkedHashMap<String, String>();
    private volatile boolean running = true;
    private volatile KafkaConsumer<byte[], byte[]> activeConsumer;

    KafkaConsumerWorker(AppConfig.KafkaSourceConfig source,
                        int workerIndex,
                        AppConfig.RuntimeConfig runtime,
                        Map<String, TopicMapping> allMappings,
                        KafkaRecordProcessor processor) {
        this.source = source;
        this.workerIndex = workerIndex;
        this.runtime = runtime;
        this.allMappings = allMappings;
        this.processor = processor;
        for (AppConfig.TopicSubscription topic : source.getTopics()) {
            topicMappings.put(topic.getName(), topic.getMapping());
        }
    }

    @Override
    public void run() {
        while (running) {
            try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(consumerProperties())) {
                activeConsumer = consumer;
                consumer.subscribe(new ArrayList<String>(topicMappings.keySet()));
                LOGGER.info("Kafka consumer started: source={}, worker={}, topics={}",
                        source.getId(), workerIndex, topicMappings.keySet());
                pollLoop(consumer);
            } catch (WakeupException e) {
                if (running) {
                    LOGGER.warn("Kafka consumer was unexpectedly woken: source={}, worker={}",
                            source.getId(), workerIndex, e);
                }
            } catch (RuntimeException e) {
                if (running) {
                    LOGGER.error("Kafka consumer failed; it will be recreated: source={}, worker={}",
                            source.getId(), workerIndex, e);
                    pauseBeforeRestart();
                }
            } finally {
                activeConsumer = null;
            }
        }
        LOGGER.info("Kafka consumer stopped: source={}, worker={}", source.getId(), workerIndex);
    }

    void stop() {
        running = false;
        KafkaConsumer<byte[], byte[]> consumer = activeConsumer;
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void pollLoop(KafkaConsumer<byte[], byte[]> consumer) {
        while (running) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(runtime.getPollTimeoutMs()));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (!running) {
                    return;
                }
                processAndCommit(consumer, record);
            }
        }
    }

    private void processAndCommit(KafkaConsumer<byte[], byte[]> consumer,
                                  ConsumerRecord<byte[], byte[]> record) {
        String mappingName = topicMappings.get(record.topic());
        TopicMapping mapping = allMappings.get(mappingName);
        KafkaEnvelope envelope = new KafkaEnvelope(
                source.getId(), record.topic(), record.partition(), record.offset(), record.timestamp(), record.value());
        try {
            processor.process(envelope, mappingName, mapping);
        } catch (FailureStoreException e) {
            throw new FailurePersistenceRuntimeException(
                    "Failed message could not be recorded; offset will not be committed", e);
        }

        TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        OffsetAndMetadata nextOffset = new OffsetAndMetadata(record.offset() + 1L);
        consumer.commitSync(Collections.singletonMap(partition, nextOffset));
    }

    private Properties consumerProperties() {
        Properties properties = new Properties();
        if (source.getProperties() != null) {
            properties.putAll(source.getProperties());
        }
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, join(source.getBootstrapServers()));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, source.getGroupId());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, source.getId() + "-" + workerIndex);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        if (!properties.containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)) {
            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        }
        return properties;
    }

    private String join(List<String> values) {
        StringBuilder joined = new StringBuilder();
        for (String value : values) {
            if (joined.length() > 0) {
                joined.append(',');
            }
            joined.append(value);
        }
        return joined.toString();
    }

    private void pauseBeforeRestart() {
        try {
            Thread.sleep(runtime.getConsumerRestartBackoffMs());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    private static final class FailurePersistenceRuntimeException extends RuntimeException {
        private FailurePersistenceRuntimeException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}

