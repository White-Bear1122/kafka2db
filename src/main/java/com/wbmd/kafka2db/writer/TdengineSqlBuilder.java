package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.config.TopicMapping.FieldMapping;
import com.wbmd.kafka2db.mapping.MappingException;
import com.wbmd.kafka2db.mapping.ValueConverter;
import com.wbmd.kafka2db.parser.ParsedMessage;
import com.wbmd.kafka2db.parser.ParsedRecord;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TdengineSqlBuilder {
    private final ValueConverter converter;

    public TdengineSqlBuilder(ValueConverter converter) {
        this.converter = converter;
    }

    public String build(TopicMapping mapping, ParsedMessage message) {
        TopicMapping.TdengineMapping target = mapping.getTdengine();
        List<FieldMapping> valueFields = orderedValueFields(target);
        Map<String, SubtableBatch> subtables = new LinkedHashMap<String, SubtableBatch>();

        for (ParsedRecord record : message.getRecords()) {
            Map<String, Object> valuesByColumn = projectAll(target, message, record);
            Object suffix = valuesByColumn.get(target.getSubtableSuffix());
            if (suffix == null) {
                throw new MappingException("TDengine subtable suffix is null: " + target.getSubtableSuffix());
            }
            String subtable = target.getSubtablePrefix() + suffix.toString().trim();
            if (!subtable.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                throw new MappingException("Invalid TDengine subtable name: " + subtable);
            }

            List<Object> tags = values(target.getTags(), valuesByColumn);
            List<Object> row = values(valueFields, valuesByColumn);
            if (row.get(0) == null) {
                throw new MappingException("TDengine timestamp cannot be null: " + target.getTimestampField());
            }
            SubtableBatch batch = subtables.get(subtable);
            if (batch == null) {
                batch = new SubtableBatch(tags);
                subtables.put(subtable, batch);
            } else if (!batch.tags.equals(tags)) {
                throw new MappingException("TDengine tags differ for subtable " + subtable);
            }
            batch.rows.add(row);
        }

        StringBuilder sql = new StringBuilder("INSERT INTO ");
        boolean firstSubtable = true;
        for (Map.Entry<String, SubtableBatch> entry : subtables.entrySet()) {
            if (!firstSubtable) {
                sql.append(' ');
            }
            firstSubtable = false;
            sql.append(entry.getKey())
                    .append(" USING ").append(target.getTable())
                    .append(" TAGS (");
            appendValues(sql, entry.getValue().tags);
            sql.append(") VALUES ");
            for (int rowIndex = 0; rowIndex < entry.getValue().rows.size(); rowIndex++) {
                if (rowIndex > 0) {
                    sql.append(' ');
                }
                sql.append('(');
                appendValues(sql, entry.getValue().rows.get(rowIndex));
                sql.append(')');
            }
        }
        return sql.toString();
    }

    private Map<String, Object> projectAll(TopicMapping.TdengineMapping target,
                                           ParsedMessage message,
                                           ParsedRecord record) {
        Map<String, Object> values = new LinkedHashMap<String, Object>();
        project(target.getTags(), message, record, values);
        project(target.getFields(), message, record, values);
        return values;
    }

    private void project(List<FieldMapping> fields,
                         ParsedMessage message,
                         ParsedRecord record,
                         Map<String, Object> output) {
        for (FieldMapping field : fields) {
            output.put(field.getColumn(), converter.convert(message.getValue(record, field.getSource()), field));
        }
    }

    private List<FieldMapping> orderedValueFields(TopicMapping.TdengineMapping target) {
        List<FieldMapping> result = new ArrayList<FieldMapping>();
        for (FieldMapping field : target.getFields()) {
            if (field.getColumn().equals(target.getTimestampField())) {
                result.add(field);
                break;
            }
        }
        for (FieldMapping field : target.getFields()) {
            if (!field.getColumn().equals(target.getTimestampField())) {
                result.add(field);
            }
        }
        return result;
    }

    private List<Object> values(List<FieldMapping> fields, Map<String, Object> valuesByColumn) {
        List<Object> result = new ArrayList<Object>(fields.size());
        for (FieldMapping field : fields) {
            result.add(valuesByColumn.get(field.getColumn()));
        }
        return result;
    }

    private void appendValues(StringBuilder sql, List<Object> values) {
        for (int index = 0; index < values.size(); index++) {
            if (index > 0) {
                sql.append(',');
            }
            sql.append(SqlLiteralFormatter.format(values.get(index)));
        }
    }

    private static final class SubtableBatch {
        private final List<Object> tags;
        private final List<List<Object>> rows = new ArrayList<List<Object>>();

        private SubtableBatch(List<Object> tags) {
            this.tags = tags;
        }
    }
}

