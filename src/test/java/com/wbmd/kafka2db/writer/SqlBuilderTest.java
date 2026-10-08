package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.mapping.ValueConverter;
import com.wbmd.kafka2db.parser.KafkaMessageParser;
import com.wbmd.kafka2db.parser.ParsedMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlBuilderTest {
    private ParsedMessage message;

    @BeforeEach
    void setUp() throws Exception {
        TopicMapping.SourceSchema source = new TopicMapping.SourceSchema();
        source.setHeaderFieldCount(3);
        source.setBodyFieldCount(6);
        String payload = "mine1;Mine Name;2026-08-03 15:39:38~"
                + "point1;1501;position1;12.50;0;2026-08-03 15:39:38~"
                + "point2;1501;position2;20.00;1;2026-08-03 15:39:39~||";
        message = new KafkaMessageParser().parse(
                payload.getBytes(StandardCharsets.UTF_8), source, new AppConfig.RuntimeConfig());
    }

    @Test
    void buildsOneOceanBaseMultiValueUpsert() {
        TopicMapping mapping = new TopicMapping();
        TopicMapping.OceanBaseMapping target = new TopicMapping.OceanBaseMapping();
        target.setDatabase("mine_db");
        target.setTable("sensor_latest");
        target.setUniqueKeyField("point_code");
        target.setFields(Arrays.asList(
                field("mine_code", "headerField1", "string"),
                field("point_code", "boreholeField1", "string"),
                field("measured_value", "boreholeField4", "decimal")));
        mapping.setOceanbase(target);

        PreparedSql prepared = new OceanBaseSqlBuilder(new ValueConverter()).build(mapping, message);

        assertEquals("INSERT INTO `mine_db`.`sensor_latest` (`mine_code`,`point_code`,`measured_value`) "
                        + "VALUES (?,?,?),(?,?,?) ON DUPLICATE KEY UPDATE "
                        + "`mine_code`=VALUES(`mine_code`),`measured_value`=VALUES(`measured_value`)",
                prepared.getSql());
        assertEquals(6, prepared.getParameters().size());
        assertEquals("mine1", prepared.getParameters().get(0));
        assertEquals(new BigDecimal("20.00"), prepared.getParameters().get(5));
    }

    @Test
    void buildsOneTdengineMultiSubtableInsertWithIndependentFields() {
        TopicMapping mapping = new TopicMapping();
        TopicMapping.TdengineMapping target = new TopicMapping.TdengineMapping();
        target.setTable("st_sensor_history");
        target.setSubtablePrefix("d_");
        target.setSubtableSuffix("point_code");
        target.setTimestampField("data_time");
        target.setTags(Arrays.asList(
                field("mine_code", "headerField1", "string"),
                field("point_code", "boreholeField1", "string")));
        target.setFields(Arrays.asList(
                field("data_time", "boreholeField6", "datetime"),
                field("measured_value", "boreholeField4", "decimal")));
        mapping.setTdengine(target);

        String sql = new TdengineSqlBuilder(new ValueConverter()).build(mapping, message);

        assertTrue(sql.startsWith("INSERT INTO d_point1 USING st_sensor_history TAGS ('mine1','point1') "));
        assertTrue(sql.contains("VALUES ('2026-08-03 15:39:38.000',12.50)"));
        assertTrue(sql.contains("d_point2 USING st_sensor_history TAGS ('mine1','point2')"));
        assertTrue(sql.contains("('2026-08-03 15:39:39.000',20.00)"));
    }

    private TopicMapping.FieldMapping field(String column, String source, String type) {
        TopicMapping.FieldMapping field = new TopicMapping.FieldMapping();
        field.setColumn(column);
        field.setSource(source);
        field.setType(type);
        return field;
    }
}

