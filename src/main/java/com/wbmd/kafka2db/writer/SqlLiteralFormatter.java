package com.wbmd.kafka2db.writer;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

final class SqlLiteralFormatter {
    private SqlLiteralFormatter() {
    }

    static String format(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Timestamp) {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            return quote(formatter.format((Timestamp) value));
        }
        if (value instanceof Date) {
            return quote(value.toString());
        }
        return quote(value.toString());
    }

    private static String quote(String value) {
        return "'" + value.replace("'", "''") + "'";
    }
}

