package top.byteinfo.iter;

import org.apache.commons.lang3.StringUtils;
import top.byteinfo.iter.schema.DataBase;
import top.byteinfo.iter.schema.Schema;
import top.byteinfo.iter.schema.ServerCaseSensitivity;
import top.byteinfo.iter.schema.Table;
import top.byteinfo.source.maxwell.schema.CustomDatabase;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;


public class SchemaCapture {
    public static final HashSet<String> IGNORED_DATABASES = new HashSet<>(
            Arrays.asList("performance_schema", "information_schema")
    );
    private static final Logger logger = Logger.getLogger(SchemaCapture.class.getName());
    private final Connection connection;
    //    private final DruidPooledConnection druidPooledConnection;
    //    private final Set<String> includeDatabases;
//    private final Set<String> includeTables;
    private final ServerCaseSensitivity sensitivity;
    private final PreparedStatement dbPreparedStatement;
    private final PreparedStatement tablePreparedStatement;
    //    private final boolean isMySQLAtLeast56;
    private final PreparedStatement columnPreparedStatement;
    private final PreparedStatement pkPreparedStatement;
    String dbCaptureQuery_old =
            "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME";
    String dbCaptureQuery = """
            SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME
            """;
    String tblSql_old = "SELECT TABLES.TABLE_NAME, CCSA.CHARACTER_SET_NAME "
            + "FROM INFORMATION_SCHEMA.TABLES "
            + "JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA"
            + " ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME WHERE TABLES.TABLE_SCHEMA = ? ";
    String tblSql = """
            SELECT
                TABLES.TABLE_NAME,
                CCSA.CHARACTER_SET_NAME
            FROM
                INFORMATION_SCHEMA.TABLES
                JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME
            WHERE TABLES.TABLE_SCHEMA = ?
            """;
    String columnSql_old = "SELECT " +
            "TABLE_NAME," +
            "COLUMN_NAME, " +
            "DATA_TYPE, " +
            "CHARACTER_SET_NAME, " +
            "ORDINAL_POSITION, " +
            "COLUMN_TYPE, " +
            "DATETIME_PRECISION, " +
            "COLUMN_KEY " +
            "FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION";
    String columnSql = """
            SELECT 
            TABLE_NAME,
            COLUMN_NAME, 
            DATA_TYPE,
            CHARACTER_SET_NAME,
            ORDINAL_POSITION, 
            COLUMN_TYPE, 
            DATETIME_PRECISION, 
            COLUMN_KEY 
            FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION
            """;
    String pkSQl_old = "SELECT " +
            "TABLE_NAME, " +
            "COLUMN_NAME, " +
            "ORDINAL_POSITION " +
            "FROM information_schema.KEY_COLUMN_USAGE " +
            "WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ? " +
            "ORDER BY TABLE_NAME, ORDINAL_POSITION";
    String pkSQl = """
            SELECT
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION
            FROM information_schema.KEY_COLUMN_USAGE 
            WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME, ORDINAL_POSITION             
            """;

    public SchemaCapture(Connection connection, ServerCaseSensitivity caseSensitivity) {
//        this.druidPooledConnection = connection;
        this.sensitivity = caseSensitivity;
        this.connection = connection;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(tblSql);
            dbPreparedStatement = connection.prepareStatement(dbCaptureQuery);
            tablePreparedStatement = connection.prepareStatement(tblSql);
            columnPreparedStatement = connection.prepareStatement(columnSql);
            pkPreparedStatement = connection.prepareStatement(pkSQl);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public Schema capture() {

        List<CustomDatabase> customDatabaseList = new ArrayList<>();

        List<DataBase> dbList = new ArrayList<>();

        try (ResultSet rs = dbPreparedStatement.executeQuery()) {
            while (rs.next()) {

                String dbName = rs.getString("SCHEMA_NAME");
                String charset = rs.getString("DEFAULT_CHARACTER_SET_NAME");

                if (IGNORED_DATABASES.contains(dbName))
                    continue;
                DataBase dataBase = new DataBase(dbName, charset, this.sensitivity);
                dbList.add(dataBase);

                CustomDatabase db = new CustomDatabase(dbName, charset);
                customDatabaseList.add(db);
            }

            String charset = captureDefaultCharset();
            for (DataBase database : dbList) {
                captureDatabase(database);
            }


            Map<String, DataBase> databaseMap = dbList.stream().collect(Collectors.toMap(DataBase::getName, item -> item));
            Schema schema = new Schema(databaseMap, charset, this.sensitivity);

            return schema;

        } catch (Exception e) {
            logger.info(e.toString());
        }
        return null;
    }

    private String captureDefaultCharset() throws SQLException {
//        LOGGER.debug("Capturing Default Charset");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select @@character_set_server")) {
            rs.next();
            return rs.getString("@@character_set_server");
        }
    }

    private void captureDatabase(DataBase database) {

        HashMap<String, Table> tables = null;

        try {


            tablePreparedStatement.setString(1, database.getName());

//        Sql.prepareInList(tablePreparedStatement, 2, includeTables);

            tables = new HashMap<>();

            try (ResultSet rs = tablePreparedStatement.executeQuery()) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String characterSetName = rs.getString("CHARACTER_SET_NAME");
                    Table t = database.buildTable(tableName, characterSetName);
                    tables.put(tableName, t);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        captureTables(database, tables);
    }


    private void captureTables(DataBase database, HashMap<String, Table> customTables) {

    }

    ;

    public class Sql {
        public static String inListSQL(int count) {
            return "(" + StringUtils.repeat("?", ", ", count) + ")";
        }

        public static void prepareInList(PreparedStatement s, int offset, Iterable<?> list) throws SQLException {
            for (Object o : list) {
                s.setObject(offset++, o);
            }
        }
    }
}
