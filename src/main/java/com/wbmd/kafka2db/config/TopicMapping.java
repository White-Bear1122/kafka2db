package com.wbmd.kafka2db.config;

import java.util.ArrayList;
import java.util.List;

public class TopicMapping {
    private SourceSchema source = new SourceSchema();
    private OceanBaseMapping oceanbase;
    private TdengineMapping tdengine;

    public SourceSchema getSource() {
        return source;
    }

    public void setSource(SourceSchema source) {
        this.source = source;
    }

    public OceanBaseMapping getOceanbase() {
        return oceanbase;
    }

    public void setOceanbase(OceanBaseMapping oceanbase) {
        this.oceanbase = oceanbase;
    }

    public TdengineMapping getTdengine() {
        return tdengine;
    }

    public void setTdengine(TdengineMapping tdengine) {
        this.tdengine = tdengine;
    }

    public static class SourceSchema {
        private int headerFieldCount;
        private int bodyFieldCount;
        private String charset = "UTF-8";
        private boolean removeLineBreaks = true;

        public int getHeaderFieldCount() {
            return headerFieldCount;
        }

        public void setHeaderFieldCount(int headerFieldCount) {
            this.headerFieldCount = headerFieldCount;
        }

        public int getBodyFieldCount() {
            return bodyFieldCount;
        }

        public void setBodyFieldCount(int bodyFieldCount) {
            this.bodyFieldCount = bodyFieldCount;
        }

        public String getCharset() {
            return charset;
        }

        public void setCharset(String charset) {
            this.charset = charset;
        }

        public boolean isRemoveLineBreaks() {
            return removeLineBreaks;
        }

        public void setRemoveLineBreaks(boolean removeLineBreaks) {
            this.removeLineBreaks = removeLineBreaks;
        }
    }

    public static class OceanBaseMapping {
        private String database;
        private String table;
        private String uniqueKeyField;
        private List<FieldMapping> fields = new ArrayList<FieldMapping>();

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getUniqueKeyField() {
            return uniqueKeyField;
        }

        public void setUniqueKeyField(String uniqueKeyField) {
            this.uniqueKeyField = uniqueKeyField;
        }

        public List<FieldMapping> getFields() {
            return fields;
        }

        public void setFields(List<FieldMapping> fields) {
            this.fields = fields;
        }
    }

    public static class TdengineMapping {
        private String table;
        private String subtablePrefix;
        private String subtableSuffix;
        private String timestampField;
        private List<FieldMapping> tags = new ArrayList<FieldMapping>();
        private List<FieldMapping> fields = new ArrayList<FieldMapping>();

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public String getSubtablePrefix() {
            return subtablePrefix;
        }

        public void setSubtablePrefix(String subtablePrefix) {
            this.subtablePrefix = subtablePrefix;
        }

        public String getSubtableSuffix() {
            return subtableSuffix;
        }

        public void setSubtableSuffix(String subtableSuffix) {
            this.subtableSuffix = subtableSuffix;
        }

        public String getTimestampField() {
            return timestampField;
        }

        public void setTimestampField(String timestampField) {
            this.timestampField = timestampField;
        }

        public List<FieldMapping> getTags() {
            return tags;
        }

        public void setTags(List<FieldMapping> tags) {
            this.tags = tags;
        }

        public List<FieldMapping> getFields() {
            return fields;
        }

        public void setFields(List<FieldMapping> fields) {
            this.fields = fields;
        }
    }

    public static class FieldMapping {
        private String column;
        private String source;
        private String type = "string";
        private boolean required;
        private boolean trim = true;

        public String getColumn() {
            return column;
        }

        public void setColumn(String column) {
            this.column = column;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public boolean isRequired() {
            return required;
        }

        public void setRequired(boolean required) {
            this.required = required;
        }

        public boolean isTrim() {
            return trim;
        }

        public void setTrim(boolean trim) {
            this.trim = trim;
        }
    }
}

