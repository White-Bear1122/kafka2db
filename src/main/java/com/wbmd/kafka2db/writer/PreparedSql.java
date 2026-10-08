package com.wbmd.kafka2db.writer;

import java.util.Collections;
import java.util.List;

public final class PreparedSql {
    private final String sql;
    private final List<Object> parameters;

    public PreparedSql(String sql, List<Object> parameters) {
        this.sql = sql;
        this.parameters = Collections.unmodifiableList(parameters);
    }

    public String getSql() {
        return sql;
    }

    public List<Object> getParameters() {
        return parameters;
    }
}

