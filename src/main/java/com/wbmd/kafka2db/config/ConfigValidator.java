package com.wbmd.kafka2db.config;

import com.wbmd.kafka2db.config.AppConfig.DatabaseEndpoint;
import com.wbmd.kafka2db.config.AppConfig.KafkaSourceConfig;
import com.wbmd.kafka2db.config.AppConfig.TopicSubscription;
import com.wbmd.kafka2db.config.TopicMapping.FieldMapping;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ConfigValidator {
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_$]*");
    private static final Pattern HEADER_SOURCE = Pattern.compile("headerField([1-9][0-9]*)");
    private static final Pattern BODY_SOURCE = Pattern.compile("boreholeField([1-9][0-9]*)");
    private static final Set<String> SUPPORTED_TYPES = supportedTypes();

    private ConfigValidator() {
    }

    public static void validate(AppConfig config) {
        List<String> errors = new ArrayList<String>();
        if (config == null) {
            throw new ConfigException("Configuration is empty");
        }
        validateRuntime(config, errors);
        validateMappings(config, errors);
        validateKafkaSources(config, errors);
        validateDatabaseRequirements(config, errors);
        if (!errors.isEmpty()) {
            StringBuilder message = new StringBuilder("Invalid configuration:");
            for (String error : errors) {
                message.append(System.lineSeparator()).append(" - ").append(error);
            }
            throw new ConfigException(message.toString());
        }
    }

    private static void validateRuntime(AppConfig config, List<String> errors) {
        AppConfig.RuntimeConfig runtime = config.getRuntime();
        if (runtime == null) {
            errors.add("runtime is required");
            return;
        }
        if (runtime.getPollTimeoutMs() <= 0) {
            errors.add("runtime.pollTimeoutMs must be greater than zero");
        }
        if (runtime.getMaxMessageBytes() <= 0) {
            errors.add("runtime.maxMessageBytes must be greater than zero");
        }
        if (runtime.getMaxRecordsPerMessage() <= 0) {
            errors.add("runtime.maxRecordsPerMessage must be greater than zero");
        }
        if (runtime.getConsumerRestartBackoffMs() < 0) {
            errors.add("runtime.consumerRestartBackoffMs must not be negative");
        }
        if (runtime.getRetry() == null || runtime.getRetry().getMaxRetries() < 0) {
            errors.add("runtime.retry.maxRetries must not be negative");
        } else {
            List<Long> backoff = runtime.getRetry().getBackoffMs();
            if (runtime.getRetry().getMaxRetries() > 0 && (backoff == null || backoff.isEmpty())) {
                errors.add("runtime.retry.backoffMs is required when retries are enabled");
            } else if (backoff != null) {
                for (Long delay : backoff) {
                    if (delay == null || delay.longValue() < 0L) {
                        errors.add("runtime.retry.backoffMs values must not be negative");
                        break;
                    }
                }
            }
        }
        if (runtime.getFailure() == null || isBlank(runtime.getFailure().getDirectory())) {
            errors.add("runtime.failure.directory is required");
        }
    }

    private static void validateMappings(AppConfig config, List<String> errors) {
        Map<String, TopicMapping> mappings = config.getTopicMappings();
        if (mappings == null || mappings.isEmpty()) {
            errors.add("topicMappings must contain at least one mapping");
            return;
        }
        for (Map.Entry<String, TopicMapping> entry : mappings.entrySet()) {
            String path = "topicMappings." + entry.getKey();
            TopicMapping mapping = entry.getValue();
            if (isBlank(entry.getKey()) || mapping == null) {
                errors.add(path + " is invalid");
                continue;
            }
            validateSource(mapping.getSource(), path + ".source", errors);
            if (mapping.getOceanbase() == null && mapping.getTdengine() == null) {
                errors.add(path + " must configure oceanbase, tdengine, or both");
            }
            if (mapping.getOceanbase() != null) {
                validateOceanBase(mapping, path + ".oceanbase", errors);
            }
            if (mapping.getTdengine() != null) {
                validateTdengine(mapping, path + ".tdengine", errors);
            }
        }
    }

    private static void validateSource(TopicMapping.SourceSchema source,
                                       String path,
                                       List<String> errors) {
        if (source == null) {
            errors.add(path + " is required");
            return;
        }
        if (source.getHeaderFieldCount() <= 0) {
            errors.add(path + ".headerFieldCount must be greater than zero");
        }
        if (source.getBodyFieldCount() <= 0) {
            errors.add(path + ".bodyFieldCount must be greater than zero");
        }
        try {
            if (isBlank(source.getCharset()) || !Charset.isSupported(source.getCharset())) {
                errors.add(path + ".charset is not supported: " + source.getCharset());
            }
        } catch (IllegalArgumentException e) {
            errors.add(path + ".charset is not supported: " + source.getCharset());
        }
    }

    private static void validateOceanBase(TopicMapping mapping,
                                          String path,
                                          List<String> errors) {
        TopicMapping.OceanBaseMapping target = mapping.getOceanbase();
        validateIdentifier(target.getDatabase(), path + ".database", errors);
        validateIdentifier(target.getTable(), path + ".table", errors);
        validateIdentifier(target.getUniqueKeyField(), path + ".uniqueKeyField", errors);
        validateFields(target.getFields(), mapping.getSource(), path + ".fields", errors);
        if (!containsColumn(target.getFields(), target.getUniqueKeyField())) {
            errors.add(path + ".uniqueKeyField must be present in fields");
        }
    }

    private static void validateTdengine(TopicMapping mapping,
                                         String path,
                                         List<String> errors) {
        TopicMapping.TdengineMapping target = mapping.getTdengine();
        validateIdentifier(target.getTable(), path + ".table", errors);
        validateIdentifier(target.getSubtablePrefix(), path + ".subtablePrefix", errors);
        validateIdentifier(target.getSubtableSuffix(), path + ".subtableSuffix", errors);
        validateIdentifier(target.getTimestampField(), path + ".timestampField", errors);
        validateFields(target.getTags(), mapping.getSource(), path + ".tags", errors);
        validateFields(target.getFields(), mapping.getSource(), path + ".fields", errors);

        Set<String> columns = new HashSet<String>();
        collectColumns(target.getTags(), columns, path + ".tags", errors);
        collectColumns(target.getFields(), columns, path + ".fields", errors);
        if (!columns.contains(target.getSubtableSuffix())) {
            errors.add(path + ".subtableSuffix must reference a configured target column");
        }
        if (!containsColumn(target.getFields(), target.getTimestampField())) {
            errors.add(path + ".timestampField must be present in fields");
        }
    }

    private static void validateFields(List<FieldMapping> fields,
                                       TopicMapping.SourceSchema source,
                                       String path,
                                       List<String> errors) {
        if (fields == null || fields.isEmpty()) {
            errors.add(path + " must contain at least one field");
            return;
        }
        Set<String> columns = new HashSet<String>();
        for (int index = 0; index < fields.size(); index++) {
            FieldMapping field = fields.get(index);
            String fieldPath = path + "[" + index + "]";
            if (field == null) {
                errors.add(fieldPath + " is required");
                continue;
            }
            validateIdentifier(field.getColumn(), fieldPath + ".column", errors);
            if (!isBlank(field.getColumn()) && !columns.add(field.getColumn())) {
                errors.add(fieldPath + ".column is duplicated: " + field.getColumn());
            }
            validateSourceReference(field.getSource(), source, fieldPath + ".source", errors);
            String type = field.getType() == null ? "string" : field.getType().trim().toLowerCase();
            if (!SUPPORTED_TYPES.contains(type)) {
                errors.add(fieldPath + ".type is unsupported: " + field.getType());
            }
        }
    }

    private static void validateSourceReference(String value,
                                                TopicMapping.SourceSchema source,
                                                String path,
                                                List<String> errors) {
        if (isBlank(value)) {
            errors.add(path + " is required");
            return;
        }
        if (source == null) {
            return;
        }
        if ("groupIndex".equals(value) || "recordIndex".equals(value)) {
            return;
        }
        Matcher header = HEADER_SOURCE.matcher(value);
        if (header.matches()) {
            if (Integer.parseInt(header.group(1)) > source.getHeaderFieldCount()) {
                errors.add(path + " exceeds headerFieldCount: " + value);
            }
            return;
        }
        Matcher body = BODY_SOURCE.matcher(value);
        if (body.matches()) {
            if (Integer.parseInt(body.group(1)) > source.getBodyFieldCount()) {
                errors.add(path + " exceeds bodyFieldCount: " + value);
            }
            return;
        }
        errors.add(path + " must be headerFieldN, boreholeFieldN, groupIndex, or recordIndex");
    }

    private static void validateKafkaSources(AppConfig config, List<String> errors) {
        List<KafkaSourceConfig> sources = config.getKafkaSources();
        if (sources == null || sources.isEmpty()) {
            errors.add("kafkaSources must contain at least one source");
            return;
        }
        if (sources.size() > 10) {
            errors.add("kafkaSources must not contain more than 10 sources");
        }
        Set<String> sourceIds = new HashSet<String>();
        for (int index = 0; index < sources.size(); index++) {
            KafkaSourceConfig source = sources.get(index);
            String path = "kafkaSources[" + index + "]";
            if (source == null) {
                errors.add(path + " is required");
                continue;
            }
            if (isBlank(source.getId()) || !sourceIds.add(source.getId())) {
                errors.add(path + ".id is required and must be unique");
            }
            if (source.getBootstrapServers() == null || source.getBootstrapServers().isEmpty()) {
                errors.add(path + ".bootstrapServers is required");
            }
            if (isBlank(source.getGroupId())) {
                errors.add(path + ".groupId is required");
            }
            if (source.getConcurrency() <= 0) {
                errors.add(path + ".concurrency must be greater than zero");
            }
            validateSubscriptions(source, config.getTopicMappings(), path + ".topics", errors);
        }
    }

    private static void validateSubscriptions(KafkaSourceConfig source,
                                              Map<String, TopicMapping> mappings,
                                              String path,
                                              List<String> errors) {
        if (source.getTopics() == null || source.getTopics().isEmpty()) {
            errors.add(path + " must contain at least one topic");
            return;
        }
        Set<String> topicNames = new HashSet<String>();
        for (int index = 0; index < source.getTopics().size(); index++) {
            TopicSubscription topic = source.getTopics().get(index);
            String itemPath = path + "[" + index + "]";
            if (topic == null || isBlank(topic.getName()) || !topicNames.add(topic.getName())) {
                errors.add(itemPath + ".name is required and must be unique in the Kafka source");
                continue;
            }
            if (isBlank(topic.getMapping()) || mappings == null || !mappings.containsKey(topic.getMapping())) {
                errors.add(itemPath + ".mapping does not reference an existing topicMapping");
            }
        }
    }

    private static void validateDatabaseRequirements(AppConfig config, List<String> errors) {
        boolean needsOceanBase = false;
        boolean needsTdengine = false;
        if (config.getTopicMappings() != null) {
            for (TopicMapping mapping : config.getTopicMappings().values()) {
                needsOceanBase |= mapping != null && mapping.getOceanbase() != null;
                needsTdengine |= mapping != null && mapping.getTdengine() != null;
            }
        }
        AppConfig.DatabasesConfig databases = config.getDatabases();
        if (needsOceanBase) {
            validateEndpoint(databases == null ? null : databases.getOceanbase(), "databases.oceanbase", errors);
        }
        if (needsTdengine) {
            validateEndpoint(databases == null ? null : databases.getTdengine(), "databases.tdengine", errors);
        }
    }

    private static void validateEndpoint(DatabaseEndpoint endpoint, String path, List<String> errors) {
        if (endpoint == null) {
            errors.add(path + " is required by at least one topic mapping");
            return;
        }
        if (isBlank(endpoint.getUrl())) {
            errors.add(path + ".url is required");
        }
        if (isBlank(endpoint.getDriverClass())) {
            errors.add(path + ".driverClass is required");
        }
        if (endpoint.getMaximumPoolSize() <= 0) {
            errors.add(path + ".maximumPoolSize must be greater than zero");
        }
        if (endpoint.getConnectionTimeoutMs() <= 0 || endpoint.getValidationTimeoutMs() <= 0) {
            errors.add(path + " connection and validation timeouts must be greater than zero");
        }
        if (endpoint.getValidationTimeoutMs() >= endpoint.getConnectionTimeoutMs()) {
            errors.add(path + ".validationTimeoutMs must be less than connectionTimeoutMs");
        }
        if (endpoint.getValidationTimeoutSeconds() <= 0 || endpoint.getStatementTimeoutSeconds() <= 0) {
            errors.add(path + " JDBC timeouts must be greater than zero");
        }
    }

    private static void validateIdentifier(String value, String path, List<String> errors) {
        if (isBlank(value) || !IDENTIFIER.matcher(value).matches()) {
            errors.add(path + " is not a valid database identifier: " + value);
        }
    }

    private static boolean containsColumn(List<FieldMapping> fields, String column) {
        if (fields == null) {
            return false;
        }
        for (FieldMapping field : fields) {
            if (field != null && field.getColumn() != null && field.getColumn().equals(column)) {
                return true;
            }
        }
        return false;
    }

    private static void collectColumns(List<FieldMapping> fields,
                                       Set<String> columns,
                                       String path,
                                       List<String> errors) {
        if (fields == null) {
            return;
        }
        for (FieldMapping field : fields) {
            if (field != null && field.getColumn() != null && !columns.add(field.getColumn())) {
                errors.add(path + " duplicates target column across TDengine tags/fields: " + field.getColumn());
            }
        }
    }

    private static Set<String> supportedTypes() {
        Set<String> values = new HashSet<String>();
        values.add("string");
        values.add("int");
        values.add("long");
        values.add("decimal");
        values.add("date");
        values.add("datetime");
        return values;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
