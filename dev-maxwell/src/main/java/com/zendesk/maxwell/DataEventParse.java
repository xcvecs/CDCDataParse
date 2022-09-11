package com.zendesk.maxwell;

import com.zaxxer.hikari.HikariDataSource;
import com.zendesk.maxwell.connect.ConnectionPool;
import com.zendesk.maxwell.connect.HconnectionPool;
import com.zendesk.maxwell.constant.CaseSensitivity;
import com.zendesk.maxwell.replication.BinlogPosition;
import com.zendesk.maxwell.replication.Position;
import com.zendesk.maxwell.schema.MysqlSchemaStore;
import com.zendesk.maxwell.schema.Schema;
import com.zendesk.maxwell.schema.SchemaStoreException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

public class DataEventParse {
    private static final String jdbcurl = "jdbc:mysql://localhost?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC";
    private static final String jdbcurlwith = "jdbc:mysql://localhost/maxwell?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC";

    private MysqlSchemaStore mysqlSchemaStore;

    private List<HconnectionPool> setupdatasource() {

        HikariDataSource hikariDataSource1 = new HikariDataSource();
        hikariDataSource1.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariDataSource1.setUsername("root");
        hikariDataSource1.setPassword("root");
        hikariDataSource1.setJdbcUrl(jdbcurlwith);
        HikariDataSource hikariDataSource0 = new HikariDataSource();
        hikariDataSource0.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariDataSource0.setUsername("root");
        hikariDataSource0.setPassword("root");
        hikariDataSource0.setJdbcUrl(jdbcurl);
        HconnectionPool hconnectionPool1 = new HconnectionPool(hikariDataSource1);
        HconnectionPool hconnectionPool0 = new HconnectionPool(hikariDataSource0);

        return Arrays.asList(hconnectionPool0, hconnectionPool1);
    }

    public Position getPosition(HconnectionPool hconnectionPool1) {
        Position heartbeatRead = null;
        try (
                Connection c = hconnectionPool1.getConnection();
                PreparedStatement s = c.prepareStatement("SELECT * from `positions` where server_id = ? and client_id = ?");
        ) {

            s.setLong(1, 1);
            s.setString(2, "maxwell");

            try (
                    ResultSet rs = s.executeQuery()
            ) {
                if (!rs.next()) throw new RuntimeException("rs.next()=false");
                BinlogPosition pos = new BinlogPosition(
                        null,
                        null,
                        rs.getLong("binlog_position"),
                        rs.getString("binlog_file")
                );
                heartbeatRead = new Position(pos, rs.getLong("last_heartbeat_read"));
            }
            return heartbeatRead;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public boolean setup(List<HconnectionPool> pools, Position heartbeatRead) {

        HconnectionPool hconnectionPool1 = pools.get(1);
        HconnectionPool hconnectionPool0 = pools.get(0);

        Connection connection = null;
        try {
            connection = hconnectionPool0.getConnection();
        } catch (Exception e) {

        }

        ConnectionPool maxwellConnectionPool = hconnectionPool1;
        ConnectionPool replicationConnectionPool = hconnectionPool0;
        ConnectionPool schemaConnectionPool = hconnectionPool0;
        Long serverID = 1L;
        Position initialPosition = heartbeatRead;
        CaseSensitivity caseSensitivity = MysqlStatus.captureCaseSensitivity(connection);

        Filter filter = new Filter();
        boolean readOnly = false;


        MysqlSchemaStore schemaStore = new MysqlSchemaStore(
                maxwellConnectionPool,
                replicationConnectionPool,
                schemaConnectionPool,
                serverID,
                initialPosition,
                caseSensitivity,
                filter,
                readOnly
        );
        this.mysqlSchemaStore = schemaStore;

        Schema schema;
        try {
            schema = schemaStore.getSchema();
        } catch (SchemaStoreException e) {
            throw new RuntimeException(e);
        }
        return schema != null;
    }


}
