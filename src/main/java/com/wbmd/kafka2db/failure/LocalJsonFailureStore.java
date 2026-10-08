package com.wbmd.kafka2db.failure;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;

public final class LocalJsonFailureStore implements FailureStore {
    private final Path baseDirectory;
    private final ObjectMapper objectMapper;
    private final Object lock = new Object();

    public LocalJsonFailureStore(String directory) {
        this(Paths.get(directory), new ObjectMapper());
    }

    LocalJsonFailureStore(Path baseDirectory, ObjectMapper objectMapper) {
        this.baseDirectory = baseDirectory.toAbsolutePath().normalize();
        this.objectMapper = objectMapper;
    }

    @Override
    public void record(FailureRecord failure) throws FailureStoreException {
        synchronized (lock) {
            Path dailyDirectory = baseDirectory.resolve(LocalDate.now().toString());
            Path file = dailyDirectory.resolve("failed-messages.jsonl");
            try {
                Files.createDirectories(dailyDirectory);
                String json = objectMapper.writeValueAsString(failure);
                try (BufferedWriter writer = Files.newBufferedWriter(
                        file,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND)) {
                    writer.write(json);
                    writer.newLine();
                    writer.flush();
                }
            } catch (IOException e) {
                throw new FailureStoreException("Cannot persist failed Kafka message to " + file, e);
            }
        }
    }
}
