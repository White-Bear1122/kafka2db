package com.wbmd.kafka2db.mapping;

import com.wbmd.kafka2db.config.TopicMapping;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ValueConverterTest {
    private final ValueConverter converter = new ValueConverter();

    @Test
    void convertsSupportedTypesAndEmptyToNull() {
        assertEquals(Integer.valueOf(12), converter.convert("12", field("int")));
        assertEquals(Long.valueOf(13L), converter.convert("13", field("long")));
        assertEquals(new BigDecimal("2981.00"), converter.convert("2981.00", field("decimal")));
        assertEquals(Date.valueOf("2026-08-03"), converter.convert("20260803", field("date")));
        assertEquals(Timestamp.valueOf("2026-08-03 15:39:38"),
                converter.convert("2026-08-03T15:39:38", field("datetime")));
        assertNull(converter.convert("  ", field("decimal")));
    }

    @Test
    void rejectsInvalidCalendarDates() {
        assertThrows(MappingException.class,
                () -> converter.convert("2026-02-30", field("date")));
    }

    @Test
    void enforcesRequiredFields() {
        TopicMapping.FieldMapping field = field("string");
        field.setRequired(true);
        assertThrows(MappingException.class, () -> converter.convert("", field));
    }

    private TopicMapping.FieldMapping field(String type) {
        TopicMapping.FieldMapping field = new TopicMapping.FieldMapping();
        field.setColumn("target_column");
        field.setSource("boreholeField1");
        field.setType(type);
        return field;
    }
}
