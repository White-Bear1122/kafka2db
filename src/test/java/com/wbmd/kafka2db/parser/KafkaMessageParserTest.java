package com.wbmd.kafka2db.parser;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaMessageParserTest {
    private KafkaMessageParser parser;
    private TopicMapping.SourceSchema schema;
    private AppConfig.RuntimeConfig runtime;

    @BeforeEach
    void setUp() {
        parser = new KafkaMessageParser();
        schema = new TopicMapping.SourceSchema();
        schema.setHeaderFieldCount(3);
        schema.setBodyFieldCount(6);
        schema.setCharset("UTF-8");
        schema.setRemoveLineBreaks(true);
        runtime = new AppConfig.RuntimeConfig();
    }

    @Test
    void parsesSharedHeaderAndMultipleGroups() throws Exception {
        String message = "340603002642;恒源煤矿;2026-08-03 15:39:38~"
                + "point1;1501;巷道_1#顶板激光\n;2981.00;0;2026-08-03 15:39:38~||"
                + "point2;1501;巷道_2#帮部;4370.00;0;2026-08-03 15:39:39~"
                + "point3;1501;巷道_3#顶部;2221.00;0;2026-08-03 15:39:40~||";

        ParsedMessage parsed = parser.parse(message.getBytes(StandardCharsets.UTF_8), schema, runtime);

        assertEquals("340603002642", parsed.getHeaderFields().get("headerField1"));
        assertEquals("恒源煤矿", parsed.getHeaderFields().get("headerField2"));
        assertEquals(3, parsed.getRecords().size());
        assertEquals(1, parsed.getRecords().get(0).getGroupIndex());
        assertEquals(2, parsed.getRecords().get(1).getGroupIndex());
        assertEquals(1, parsed.getRecords().get(1).getRecordIndex());
        assertEquals("巷道_1#顶板激光", parsed.getRecords().get(0).getValue("boreholeField3"));
        assertEquals("point3", parsed.getRecords().get(2).getValue("boreholeField1"));
        assertEquals("340603002642", parsed.getValue(parsed.getRecords().get(2), "headerField1"));
    }

    @Test
    void preservesEmptyFieldsWhenCounting() throws Exception {
        String message = "mine;name;20260803153938~point1;1501;;2981.00;;20260803153938~||";

        ParsedMessage parsed = parser.parse(message.getBytes(StandardCharsets.UTF_8), schema, runtime);

        assertEquals("", parsed.getRecords().get(0).getValue("boreholeField3"));
        assertEquals("", parsed.getRecords().get(0).getValue("boreholeField5"));
    }

    @Test
    void rejectsWrongBodyFieldCount() {
        String message = "mine;name;20260803153938~point1;1501;position;2981.00;0~||";

        MessageParseException error = assertThrows(MessageParseException.class,
                () -> parser.parse(message.getBytes(StandardCharsets.UTF_8), schema, runtime));

        assertEquals(true, error.getMessage().contains("field count mismatch"));
    }

    @Test
    void rejectsContentAfterFinalGroupTerminator() {
        String message = "mine;name;20260803153938~point1;1501;position;2981.00;0;20260803153938~||tail";

        assertThrows(MessageParseException.class,
                () -> parser.parse(message.getBytes(StandardCharsets.UTF_8), schema, runtime));
    }
}

