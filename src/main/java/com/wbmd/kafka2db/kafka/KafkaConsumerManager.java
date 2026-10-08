package com.wbmd.kafka2db.kafka;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.processing.KafkaRecordProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class KafkaConsumerManager implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerManager.class);

    private final List<KafkaConsumerWorker> workers = new ArrayList<KafkaConsumerWorker>();
    private final List<Thread> threads = new ArrayList<Thread>();

    public KafkaConsumerManager(AppConfig config, KafkaRecordProcessor processor) {
        for (AppConfig.KafkaSourceConfig source : config.getKafkaSources()) {
            for (int index = 1; index <= source.getConcurrency(); index++) {
                KafkaConsumerWorker worker = new KafkaConsumerWorker(
                        source, index, config.getRuntime(), config.getTopicMappings(), processor);
                workers.add(worker);
                Thread thread = new Thread(worker, "kafka-" + source.getId() + "-" + index);
                thread.setDaemon(false);
                threads.add(thread);
            }
        }
    }

    public void start() {
        for (Thread thread : threads) {
            thread.start();
        }
    }

    @Override
    public void close() {
        for (KafkaConsumerWorker worker : workers) {
            worker.stop();
        }
        for (Thread thread : threads) {
            try {
                thread.join(10000L);
                if (thread.isAlive()) {
                    LOGGER.warn("Kafka consumer did not stop within 10 seconds: {}", thread.getName());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
