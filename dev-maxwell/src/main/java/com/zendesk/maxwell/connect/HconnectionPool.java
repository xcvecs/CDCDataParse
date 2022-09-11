package com.zendesk.maxwell.connect;

import com.zaxxer.hikari.HikariDataSource;
import com.zendesk.maxwell.exception.DuplicateProcessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HconnectionPool implements ConnectionPool {

    private static final Logger log = Logger.getLogger(HconnectionPool.class.getName());
    private final HikariDataSource dataSource;

    public HconnectionPool(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    @Override
    public void release() {
        dataSource.close();
    }

    @Override
    public void probe() throws SQLException {
        Connection connection = dataSource.getConnection();
        if (connection.isValid(1000)) return;
        else throw new RuntimeException("probe()");
    }

    @Override
    public void withSQLRetry(int nTries, RetryableSQLFunction<Connection> inner) throws SQLException, NoSuchElementException, DuplicateProcessException {

        try (final Connection c = getConnection()) {
            inner.apply(c);
            return;
        } catch (SQLException e) {
            if (nTries > 0) {

                log.log(Level.ALL,"got SQL Exception: {}, retrying..."+e.getLocalizedMessage());
                withSQLRetry(nTries - 1, inner);
            } else {
                throw (e);
            }
        }
    }
}
