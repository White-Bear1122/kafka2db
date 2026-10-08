package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.parser.ParsedMessage;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class TdengineWriter implements TargetWriter {
    private final DataSource dataSource;
    private final AppConfig.DatabaseEndpoint endpoint;
    private final TdengineSqlBuilder sqlBuilder;

    public TdengineWriter(DataSource dataSource,
                          AppConfig.DatabaseEndpoint endpoint,
                          TdengineSqlBuilder sqlBuilder) {
        this.dataSource = dataSource;
        this.endpoint = endpoint;
        this.sqlBuilder = sqlBuilder;
    }

    @Override
    public TargetType getTargetType() {
        return TargetType.TDENGINE;
    }

    @Override
    public void write(TopicMapping mapping, ParsedMessage message) throws SQLException {
        String sql = sqlBuilder.build(mapping, message);
        try {
            execute(sql);
        } catch (SQLException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SQLException("Unexpected TDengine JDBC failure", e);
        }
    }

    private void execute(String sql) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            if (!connection.isValid(endpoint.getValidationTimeoutSeconds())) {
                throw new SQLException("TDengine connection is not valid");
            }
            try (Statement statement = connection.createStatement()) {
                statement.setQueryTimeout(endpoint.getStatementTimeoutSeconds());
                statement.executeUpdate(sql);
            }
        }
    }
}
