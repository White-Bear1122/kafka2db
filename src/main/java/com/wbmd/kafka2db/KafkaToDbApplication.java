package com.wbmd.kafka2db;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.ConfigLoader;
import com.wbmd.kafka2db.failure.LocalJsonFailureStore;
import com.wbmd.kafka2db.kafka.KafkaConsumerManager;
import com.wbmd.kafka2db.mapping.ValueConverter;
import com.wbmd.kafka2db.parser.KafkaMessageParser;
import com.wbmd.kafka2db.processing.KafkaRecordProcessor;
import com.wbmd.kafka2db.retry.RetryExecutor;
import com.wbmd.kafka2db.writer.DatabaseResources;
import com.wbmd.kafka2db.writer.DatabaseWriteCoordinator;
import com.wbmd.kafka2db.writer.OceanBaseSqlBuilder;
import com.wbmd.kafka2db.writer.OceanBaseWriter;
import com.wbmd.kafka2db.writer.TargetWriter;
import com.wbmd.kafka2db.writer.TdengineSqlBuilder;
import com.wbmd.kafka2db.writer.TdengineWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaToDbApplication {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToDbApplication.class);

    private KafkaToDbApplication() {
    }

    public static void main(String[] args) {
        AppConfig config = ConfigLoader.load(args);
        final DatabaseResources databases = new DatabaseResources(config);
        List<TargetWriter> writers = createWriters(config, databases);
        DatabaseWriteCoordinator coordinator = new DatabaseWriteCoordinator(
                writers, new RetryExecutor(config.getRuntime().getRetry()));
        KafkaRecordProcessor processor = new KafkaRecordProcessor(
                config.getRuntime(),
                new KafkaMessageParser(),
                coordinator,
                new LocalJsonFailureStore(config.getRuntime().getFailure().getDirectory()));
        final KafkaConsumerManager consumers = new KafkaConsumerManager(config, processor);
        final AtomicBoolean closed = new AtomicBoolean(false);

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                closeOnce(closed, consumers, databases);
            }
        }, "kafka2db-shutdown"));

        try {
            consumers.start();
            LOGGER.info("kafka2db started with {} Kafka source(s)", config.getKafkaSources().size());
        } catch (RuntimeException e) {
            closeOnce(closed, consumers, databases);
            throw e;
        }
    }

    private static List<TargetWriter> createWriters(AppConfig config, DatabaseResources databases) {
        ValueConverter converter = new ValueConverter();
        List<TargetWriter> writers = new ArrayList<TargetWriter>();
        if (databases.getTdengine() != null) {
            writers.add(new TdengineWriter(
                    databases.getTdengine(),
                    config.getDatabases().getTdengine(),
                    new TdengineSqlBuilder(converter)));
        }
        if (databases.getOceanbase() != null) {
            writers.add(new OceanBaseWriter(
                    databases.getOceanbase(),
                    config.getDatabases().getOceanbase(),
                    new OceanBaseSqlBuilder(converter)));
        }
        return writers;
    }

    private static void closeOnce(AtomicBoolean closed,
                                  KafkaConsumerManager consumers,
                                  DatabaseResources databases) {
        if (closed.compareAndSet(false, true)) {
            consumers.close();
            databases.close();
            LOGGER.info("kafka2db stopped");
        }
    }
}
