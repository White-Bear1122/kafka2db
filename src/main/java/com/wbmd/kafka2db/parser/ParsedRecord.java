package com.wbmd.kafka2db.parser;

import java.util.Collections;
import java.util.Map;

public final class ParsedRecord {
    private final int groupIndex;
    private final int recordIndex;
    private final Map<String, String> bodyFields;

    public ParsedRecord(int groupIndex, int recordIndex, Map<String, String> bodyFields) {
        this.groupIndex = groupIndex;
        this.recordIndex = recordIndex;
        this.bodyFields = Collections.unmodifiableMap(bodyFields);
    }

    public int getGroupIndex() {
        return groupIndex;
    }

    public int getRecordIndex() {
        return recordIndex;
    }

    public Map<String, String> getBodyFields() {
        return bodyFields;
    }

    public String getValue(String source) {
        if ("groupIndex".equals(source)) {
            return Integer.toString(groupIndex);
        }
        if ("recordIndex".equals(source)) {
            return Integer.toString(recordIndex);
        }
        return bodyFields.get(source);
    }
}

