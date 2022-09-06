package top.byteinfo;

import top.byteinfo.schema.ServerCaseSensitivity;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class DataBaseServerStatus {

    private final Connection connection;

    public DataBaseServerStatus(Connection connection) {
        this.connection = connection;
    }
    private String baseSqlStatement(String variableName) {
        return "SHOW VARIABLES LIKE '" + variableName + "'";
    }


    /**
     * Class with some utility functions for querying mysql server state
     */
    public static class MaxwellMysqlStatus {

        static final Logger log = Logger.getLogger(MaxwellMysqlStatus.class.getName());
        private Connection connection;

        public MaxwellMysqlStatus(Connection c) {
            this.connection = c;
        }

        /**
         * Verify that replication is in the expected state:
         *
         * <ol>
         *     <li>Check that a serverID is set</li>
         *     <li>check that binary logging is on</li>
         *     <li>Check that the binlog_format is "ROW"</li>
         *     <li>Warn if binlog_row_image is MINIMAL</li>
         * </ol>
         *
         * @param c a JDBC connection
         * @throws SQLException if the database has issues
         */
        public static void ensureReplicationMysqlState(Connection c) throws SQLException {
            MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

            m.ensureServerIDIsSet();
            m.ensureVariableState("log_bin", "ON");
            m.ensureVariableState("binlog_format", "ROW");
            m.ensureRowImageFormat();
        }

        /**
         * Verify that the maxwell database is in the expected state
         *
         * @param c a JDBC connection
         * @throws SQLException if we have database issues
         */
        public static void ensureMaxwellMysqlState(Connection c) throws SQLException {
            MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

            m.ensureVariableState("read_only", "OFF");
        }

        /**
         * Verify that we can safely turn on maxwell GTID mode
         *
         * @param c a JDBC connection
         * @throws SQLException if we have db troubles
         */
        public static void ensureGtidMysqlState(Connection c) throws SQLException {
            MaxwellMysqlStatus m = new MaxwellMysqlStatus(c);

            m.ensureVariableState("gtid_mode", "ON");
            m.ensureVariableState("log_slave_updates", "ON");
            m.ensureVariableState("enforce_gtid_consistency", "ON");
        }

        /**
         * Return an enum representing the current case sensitivity of the server
         *
         * @param c a JDBC connection
         * @return case sensitivity
         * @throws SQLException if we have db troubles
         */
        public static ServerCaseSensitivity captureCaseSensitivity(Connection c) throws SQLException {
            final int value;
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery("select @@lower_case_table_names")) {
                if (!rs.next())
                    throw new RuntimeException("Could not retrieve @@lower_case_table_names!");
                value = rs.getInt(1);
            }

            switch (value) {
                case 0:
                    return ServerCaseSensitivity.CASE_SENSITIVE;
                case 1:
                    return ServerCaseSensitivity.CONVERT_TO_LOWER;
                case 2:
                    return ServerCaseSensitivity.CONVERT_ON_COMPARE;
                default:
                    throw new RuntimeException("Unknown value for @@lower_case_table_names: " + value);
            }
        }

        private String sqlStatement(String variableName) {
            return "SHOW VARIABLES LIKE '" + variableName + "'";
        }

        private String getVariableState(String variableName, boolean throwOnMissing) throws SQLException {
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sqlStatement(variableName))) {
                String status;
                if (!rs.next()) {
                    if (throwOnMissing) {
                        throw new RuntimeException("Could not check state for Mysql variable: " + variableName);
                    } else {
                        return null;
                    }
                }

                status = rs.getString("Value");
                return status;
            }
        }

        private void ensureVariableState(String variable, String state) throws SQLException {
            if (!getVariableState(variable, true).equals(state)) {
                throw new RuntimeException("variable " + variable + " must be set to '" + state + "'");
            }
        }

        private void ensureServerIDIsSet() throws SQLException {
            String id = getVariableState("server_id", false);
            if ("0".equals(id)) {
                throw new RuntimeException("server_id is '0'.  Maxwell will not function without a server_id being set.");
            }
        }

        private void ensureRowImageFormat() throws SQLException {
            String rowImageFormat = getVariableState("binlog_row_image", false);
            if (rowImageFormat == null) // only present in mysql 5.6+
                return;

            if (rowImageFormat.equals("MINIMAL")) {
                log.warning("Warning: binlog_row_image is set to MINIMAL.  This may not be what you want.");
                log.warning("See http://maxwells-daemon.io/compat for more information.");
            }
        }
    }
}
