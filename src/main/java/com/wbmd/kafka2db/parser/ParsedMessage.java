package com.wbmd.kafka2db.parser;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class ParsedMessage {
    private final Map<String, String> headerFields;
    private final List<ParsedRecord> records;

    public ParsedMessage(Map<String, String> headerFields, List<ParsedRecord> records) {
        this.headerFields = Collections.unmodifiableMap(headerFields);
        this.records = Collections.unmodifiableList(records);
    }

    public Map<String, String> getHeaderFields() {
        return headerFields;
    }

    public List<ParsedRecord> getRecords() {
        return records;
    }

    public String getValue(ParsedRecord record, String source) {
        if (headerFields.containsKey(source)) {
            return headerFields.get(source);
        }
        return record.getValue(source);
    }
}

