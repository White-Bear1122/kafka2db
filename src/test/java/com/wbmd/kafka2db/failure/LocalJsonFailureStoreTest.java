package com.wbmd.kafka2db.failure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wbmd.kafka2db.processing.KafkaEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LocalJsonFailureStoreTest {
    @TempDir
    Path tempDirectory;

    @Test
    void storesTheCompleteRawMessageAsUtf8JsonLine() throws Exception {
        String raw = "煤矿;名称;2026-08-03 15:39:38~测点;1501;位置\n;1.0;0;时间~||";
        KafkaEnvelope envelope = new KafkaEnvelope(
                "source-a", "topic-a", 2, 18L, 123456L, raw.getBytes(StandardCharsets.UTF_8));
        FailureRecord failure = new FailureRecord(envelope, "mapping-a", "database");
        failure.addSuccess("tdengine", 1);
        failure.addFailure("oceanbase", 4, new IllegalStateException("database unavailable"));
        failure.setRawMessage(raw);

        new LocalJsonFailureStore(tempDirectory.toString()).record(failure);

        Path file = tempDirectory.resolve(LocalDate.now().toString()).resolve("failed-messages.jsonl");
        assertTrue(Files.isRegularFile(file));
        String line = new String(Files.readAllBytes(file), StandardCharsets.UTF_8).trim();
        JsonNode json = new ObjectMapper().readTree(line);
        assertEquals(raw, json.get("rawMessage").asText());
        assertEquals(18L, json.get("offset").asLong());
        assertEquals("tdengine", json.get("successfulTargets").get(0).asText());
        assertEquals(4, json.get("attempts").get("oceanbase").asInt());
    }
}

