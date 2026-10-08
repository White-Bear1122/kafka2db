package com.wbmd.kafka2db.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AppConfig {
    private RuntimeConfig runtime = new RuntimeConfig();
    private DatabasesConfig databases = new DatabasesConfig();
    private List<KafkaSourceConfig> kafkaSources = new ArrayList<KafkaSourceConfig>();
    private Map<String, TopicMapping> topicMappings = new LinkedHashMap<String, TopicMapping>();

    public RuntimeConfig getRuntime() {
        return runtime;
    }

    public void setRuntime(RuntimeConfig runtime) {
        this.runtime = runtime;
    }

    public DatabasesConfig getDatabases() {
        return databases;
    }

    public void setDatabases(DatabasesConfig databases) {
        this.databases = databases;
    }

    public List<KafkaSourceConfig> getKafkaSources() {
        return kafkaSources;
    }

    public void setKafkaSources(List<KafkaSourceConfig> kafkaSources) {
        this.kafkaSources = kafkaSources;
    }

    public Map<String, TopicMapping> getTopicMappings() {
        return topicMappings;
    }

    public void setTopicMappings(Map<String, TopicMapping> topicMappings) {
        this.topicMappings = topicMappings;
    }

    public static class RuntimeConfig {
        private long pollTimeoutMs = 1000L;
        private int maxMessageBytes = 10 * 1024 * 1024;
        private int maxRecordsPerMessage = 10000;
        private long consumerRestartBackoffMs = 5000L;
        private RetryConfig retry = new RetryConfig();
        private FailureConfig failure = new FailureConfig();

        public long getPollTimeoutMs() {
            return pollTimeoutMs;
        }

        public void setPollTimeoutMs(long pollTimeoutMs) {
            this.pollTimeoutMs = pollTimeoutMs;
        }

        public int getMaxMessageBytes() {
            return maxMessageBytes;
        }

        public void setMaxMessageBytes(int maxMessageBytes) {
            this.maxMessageBytes = maxMessageBytes;
        }

        public int getMaxRecordsPerMessage() {
            return maxRecordsPerMessage;
        }

        public void setMaxRecordsPerMessage(int maxRecordsPerMessage) {
            this.maxRecordsPerMessage = maxRecordsPerMessage;
        }

        public long getConsumerRestartBackoffMs() {
            return consumerRestartBackoffMs;
        }

        public void setConsumerRestartBackoffMs(long consumerRestartBackoffMs) {
            this.consumerRestartBackoffMs = consumerRestartBackoffMs;
        }

        public RetryConfig getRetry() {
            return retry;
        }

        public void setRetry(RetryConfig retry) {
            this.retry = retry;
        }

        public FailureConfig getFailure() {
            return failure;
        }

        public void setFailure(FailureConfig failure) {
            this.failure = failure;
        }
    }

    public static class RetryConfig {
        private int maxRetries = 3;
        private List<Long> backoffMs = defaultBackoff();

        private static List<Long> defaultBackoff() {
            List<Long> values = new ArrayList<Long>();
            values.add(1000L);
            values.add(2000L);
            values.add(4000L);
            return values;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public List<Long> getBackoffMs() {
            return backoffMs;
        }

        public void setBackoffMs(List<Long> backoffMs) {
            this.backoffMs = backoffMs;
        }
    }

    public static class FailureConfig {
        private String directory = "./runError";
        private boolean includeRawMessage = true;

        public String getDirectory() {
            return directory;
        }

        public void setDirectory(String directory) {
            this.directory = directory;
        }

        public boolean isIncludeRawMessage() {
            return includeRawMessage;
        }

        public void setIncludeRawMessage(boolean includeRawMessage) {
            this.includeRawMessage = includeRawMessage;
        }
    }

    public static class DatabasesConfig {
        private DatabaseEndpoint oceanbase;
        private DatabaseEndpoint tdengine;

        public DatabaseEndpoint getOceanbase() {
            return oceanbase;
        }

        public void setOceanbase(DatabaseEndpoint oceanbase) {
            this.oceanbase = oceanbase;
        }

        public DatabaseEndpoint getTdengine() {
            return tdengine;
        }

        public void setTdengine(DatabaseEndpoint tdengine) {
            this.tdengine = tdengine;
        }
    }

    public static class DatabaseEndpoint {
        private String url;
        private String user;
        private String password;
        private String driverClass;
        private int maximumPoolSize = 10;
        private long connectionTimeoutMs = 10000L;
        private long validationTimeoutMs = 5000L;
        private int validationTimeoutSeconds = 5;
        private int statementTimeoutSeconds = 60;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getDriverClass() {
            return driverClass;
        }

        public void setDriverClass(String driverClass) {
            this.driverClass = driverClass;
        }

        public int getMaximumPoolSize() {
            return maximumPoolSize;
        }

        public void setMaximumPoolSize(int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
        }

        public long getConnectionTimeoutMs() {
            return connectionTimeoutMs;
        }

        public void setConnectionTimeoutMs(long connectionTimeoutMs) {
            this.connectionTimeoutMs = connectionTimeoutMs;
        }

        public long getValidationTimeoutMs() {
            return validationTimeoutMs;
        }

        public void setValidationTimeoutMs(long validationTimeoutMs) {
            this.validationTimeoutMs = validationTimeoutMs;
        }

        public int getValidationTimeoutSeconds() {
            return validationTimeoutSeconds;
        }

        public void setValidationTimeoutSeconds(int validationTimeoutSeconds) {
            this.validationTimeoutSeconds = validationTimeoutSeconds;
        }

        public int getStatementTimeoutSeconds() {
            return statementTimeoutSeconds;
        }

        public void setStatementTimeoutSeconds(int statementTimeoutSeconds) {
            this.statementTimeoutSeconds = statementTimeoutSeconds;
        }
    }

    public static class KafkaSourceConfig {
        private String id;
        private List<String> bootstrapServers = new ArrayList<String>();
        private String groupId;
        private int concurrency = 1;
        private Map<String, Object> properties = new LinkedHashMap<String, Object>();
        private List<TopicSubscription> topics = new ArrayList<TopicSubscription>();

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public List<String> getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(List<String> bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        public Map<String, Object> getProperties() {
            return properties;
        }

        public void setProperties(Map<String, Object> properties) {
            this.properties = properties;
        }

        public List<TopicSubscription> getTopics() {
            return topics;
        }

        public void setTopics(List<TopicSubscription> topics) {
            this.topics = topics;
        }
    }

    public static class TopicSubscription {
        private String name;
        private String mapping;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getMapping() {
            return mapping;
        }

        public void setMapping(String mapping) {
            this.mapping = mapping;
        }
    }
}

