package com.wbmd.kafka2db.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ExampleConfigurationTest {
    @Test
    void exampleConfigurationLoadsAndValidates() {
        AppConfig config = ConfigLoader.load(new String[]{"config/application.example.yaml"});

        assertEquals(1, config.getKafkaSources().size());
        TopicMapping mapping = config.getTopicMappings().get("mine-sensor-data-v1");
        assertNotNull(mapping.getOceanbase());
        assertNotNull(mapping.getTdengine());
        assertEquals(9, mapping.getOceanbase().getFields().size());
        assertEquals(4, mapping.getTdengine().getFields().size());
    }
}
