package com.wbmd.kafka2db.writer;

import com.wbmd.kafka2db.config.AppConfig;
import com.wbmd.kafka2db.config.TopicMapping;
import com.wbmd.kafka2db.parser.ParsedMessage;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public final class OceanBaseWriter implements TargetWriter {
    private final DataSource dataSource;
    private final AppConfig.DatabaseEndpoint endpoint;
    private final OceanBaseSqlBuilder sqlBuilder;

    public OceanBaseWriter(DataSource dataSource,
                           AppConfig.DatabaseEndpoint endpoint,
                           OceanBaseSqlBuilder sqlBuilder) {
        this.dataSource = dataSource;
        this.endpoint = endpoint;
        this.sqlBuilder = sqlBuilder;
    }

    @Override
    public TargetType getTargetType() {
        return TargetType.OCEANBASE;
    }

    @Override
    public void write(TopicMapping mapping, ParsedMessage message) throws SQLException {
        PreparedSql prepared = sqlBuilder.build(mapping, message);
        try {
            writePrepared(prepared);
        } catch (SQLException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new SQLException("Unexpected OceanBase JDBC failure", e);
        }
    }

    private void writePrepared(PreparedSql prepared) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            validate(connection);
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                execute(connection, prepared);
                connection.commit();
            } catch (SQLException e) {
                rollback(connection, e);
                throw e;
            } finally {
                restoreAutoCommit(connection, previousAutoCommit);
            }
        }
    }

    private void execute(Connection connection, PreparedSql prepared) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(prepared.getSql())) {
            statement.setQueryTimeout(endpoint.getStatementTimeoutSeconds());
            List<Object> parameters = prepared.getParameters();
            for (int index = 0; index < parameters.size(); index++) {
                statement.setObject(index + 1, parameters.get(index));
            }
            statement.executeUpdate();
        }
    }

    private void validate(Connection connection) throws SQLException {
        if (!connection.isValid(endpoint.getValidationTimeoutSeconds())) {
            throw new SQLException("OceanBase connection is not valid");
        }
    }

    private void rollback(Connection connection, SQLException original) {
        try {
            connection.rollback();
        } catch (SQLException rollbackFailure) {
            original.addSuppressed(rollbackFailure);
        }
    }

    private void restoreAutoCommit(Connection connection, boolean previousAutoCommit) throws SQLException {
        if (!connection.isClosed() && connection.getAutoCommit() != previousAutoCommit) {
            connection.setAutoCommit(previousAutoCommit);
        }
    }
}
