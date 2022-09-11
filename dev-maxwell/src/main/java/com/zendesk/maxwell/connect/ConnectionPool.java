package com.zendesk.maxwell.connect;

import com.zendesk.maxwell.exception.DuplicateProcessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.NoSuchElementException;

public interface ConnectionPool {
    @FunctionalInterface
    public interface RetryableSQLFunction<T> {
        void apply(T t) throws SQLException, NoSuchElementException, DuplicateProcessException;
    }

    Connection getConnection() throws SQLException;
    void release();

    void probe() throws SQLException;
    void withSQLRetry(int nTries, RetryableSQLFunction<Connection> inner)
            throws SQLException, NoSuchElementException, DuplicateProcessException;
}
