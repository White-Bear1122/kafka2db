package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

public final class DatabaseResources implements AutoCloseable {
    private final HikariDataSource oceanbase;
    private final HikariDataSource tdengine;

    public DatabaseResources(AppConfig config) {
        boolean needsOceanBase = false;
        boolean needsTdengine = false;
        for (TopicMapping mapping : config.getTopicMappings().values()) {
            needsOceanBase |= mapping.getOceanbase() != null;
            needsTdengine |= mapping.getTdengine() != null;
        }
        oceanbase = needsOceanBase
                ? create(config.getDatabases().getOceanbase(), "kafka2db-oceanbase") : null;
        tdengine = needsTdengine
                ? create(config.getDatabases().getTdengine(), "kafka2db-tdengine") : null;
    }

    public DataSource getOceanbase() {
        return oceanbase;
    }

    public DataSource getTdengine() {
        return tdengine;
    }

    private HikariDataSource create(AppConfig.DatabaseEndpoint endpoint, String poolName) {
        HikariConfig pool = new HikariConfig();
        pool.setPoolName(poolName);
        pool.setJdbcUrl(endpoint.getUrl());
        pool.setUsername(endpoint.getUser());
        pool.setPassword(endpoint.getPassword());
        pool.setDriverClassName(endpoint.getDriverClass());
        pool.setMaximumPoolSize(endpoint.getMaximumPoolSize());
        pool.setMinimumIdle(0);
        pool.setConnectionTimeout(endpoint.getConnectionTimeoutMs());
        pool.setValidationTimeout(endpoint.getValidationTimeoutMs());
        pool.setInitializationFailTimeout(-1L);
        return new HikariDataSource(pool);
    }

    @Override
    public void close() {
        if (tdengine != null) {
            tdengine.close();
        }
        if (oceanbase != null) {
            oceanbase.close();
        }
    }
}

