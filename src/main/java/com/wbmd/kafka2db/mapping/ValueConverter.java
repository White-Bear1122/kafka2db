package com.wbmd.kafka2db.mapping;

import com.wbmd.kafka2db.config.TopicMapping.FieldMapping;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Arrays;
import java.util.List;

public final class ValueConverter {
    private static final List<DateTimeFormatter> DATE_FORMATTERS = Arrays.asList(
            DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT),
            DateTimeFormatter.ofPattern("uuuuMMdd").withResolverStyle(ResolverStyle.STRICT)
    );
    private static final List<DateTimeFormatter> DATETIME_FORMATTERS = Arrays.asList(
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withResolverStyle(ResolverStyle.STRICT),
            DateTimeFormatter.ofPattern("uuuuMMddHHmmss").withResolverStyle(ResolverStyle.STRICT),
            DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss").withResolverStyle(ResolverStyle.STRICT)
    );

    public Object convert(String raw, FieldMapping field) {
        String normalized = raw;
        if (normalized != null && field.isTrim()) {
            normalized = normalized.trim();
        }
        if (normalized == null || normalized.trim().isEmpty()) {
            if (field.isRequired()) {
                throw new MappingException("Required source field is empty: " + field.getSource());
            }
            return null;
        }

        String type = field.getType() == null ? "string" : field.getType().trim().toLowerCase();
        try {
            if ("string".equals(type)) {
                return normalized;
            }
            if ("int".equals(type)) {
                return Integer.valueOf(normalized.trim());
            }
            if ("long".equals(type)) {
                return Long.valueOf(normalized.trim());
            }
            if ("decimal".equals(type)) {
                return new BigDecimal(normalized.trim());
            }
            if ("date".equals(type)) {
                return Date.valueOf(parseDate(normalized.trim()));
            }
            if ("datetime".equals(type)) {
                return Timestamp.valueOf(parseDateTime(normalized.trim()));
            }
        } catch (RuntimeException e) {
            throw new MappingException(
                    "Cannot convert source " + field.getSource() + " value '" + normalized
                            + "' to " + type + " for column " + field.getColumn(), e);
        }
        throw new MappingException("Unsupported field type: " + type);
    }

    private LocalDate parseDate(String value) {
        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                return LocalDate.parse(value, formatter);
            } catch (DateTimeParseException ignored) {
                // Try the next configured format.
            }
        }
        throw new DateTimeParseException("Unsupported date", value, 0);
    }

    private LocalDateTime parseDateTime(String value) {
        for (DateTimeFormatter formatter : DATETIME_FORMATTERS) {
            try {
                return LocalDateTime.parse(value, formatter);
            } catch (DateTimeParseException ignored) {
                // Try the next configured format.
            }
        }
        throw new DateTimeParseException("Unsupported datetime", value, 0);
    }
}
