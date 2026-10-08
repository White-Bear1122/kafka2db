package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.config.TopicMapping.FieldMapping;
import com.wbmd.kafka2db.mapping.ValueConverter;
import com.wbmd.kafka2db.parser.ParsedMessage;
import com.wbmd.kafka2db.parser.ParsedRecord;

import java.util.ArrayList;
import java.util.List;

public final class OceanBaseSqlBuilder {
    private final ValueConverter converter;

    public OceanBaseSqlBuilder(ValueConverter converter) {
        this.converter = converter;
    }

    public PreparedSql build(TopicMapping mapping, ParsedMessage message) {
        TopicMapping.OceanBaseMapping target = mapping.getOceanbase();
        List<FieldMapping> fields = target.getFields();
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ")
                .append(quote(target.getDatabase())).append('.').append(quote(target.getTable()))
                .append(" (");
        appendColumns(sql, fields);
        sql.append(") VALUES ");

        List<Object> parameters = new ArrayList<Object>(message.getRecords().size() * fields.size());
        for (int rowIndex = 0; rowIndex < message.getRecords().size(); rowIndex++) {
            if (rowIndex > 0) {
                sql.append(',');
            }
            sql.append('(');
            ParsedRecord record = message.getRecords().get(rowIndex);
            for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
                if (fieldIndex > 0) {
                    sql.append(',');
                }
                sql.append('?');
                FieldMapping field = fields.get(fieldIndex);
                parameters.add(converter.convert(message.getValue(record, field.getSource()), field));
            }
            sql.append(')');
        }

        sql.append(" ON DUPLICATE KEY UPDATE ");
        boolean appended = false;
        for (FieldMapping field : fields) {
            if (field.getColumn().equals(target.getUniqueKeyField())) {
                continue;
            }
            if (appended) {
                sql.append(',');
            }
            sql.append(quote(field.getColumn()))
                    .append("=VALUES(").append(quote(field.getColumn())).append(')');
            appended = true;
        }
        if (!appended) {
            sql.append(quote(target.getUniqueKeyField()))
                    .append("=VALUES(").append(quote(target.getUniqueKeyField())).append(')');
        }
        return new PreparedSql(sql.toString(), parameters);
    }

    private void appendColumns(StringBuilder sql, List<FieldMapping> fields) {
        for (int index = 0; index < fields.size(); index++) {
            if (index > 0) {
                sql.append(',');
            }
            sql.append(quote(fields.get(index).getColumn()));
        }
    }

    private String quote(String identifier) {
        return '`' + identifier + '`';
    }
}

