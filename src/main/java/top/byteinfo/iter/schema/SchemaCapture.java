package top.byteinfo.iter.schema;

import org.apache.commons.lang3.StringUtils;
import top.byteinfo.iter.schema.columndef.ColumnDef;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static top.byteinfo.iter.GlobalConstant.SQLResultColumnLabel.*;


public class SchemaCapture {
    public static final HashSet<String> IGNORED_DATABASES = new HashSet<>(Arrays.asList("performance_schema", "information_schema"));
    private static final Logger logger = Logger.getLogger(SchemaCapture.class.getName());

    /**
     * sqlParse
     */
    private final Properties sqlProperties = new Properties();
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
    String dbCaptureQuery_old = "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME";
//    String dbCaptureQuery = """
//            SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME
//            """;
    String tblSql_old = "SELECT TABLES.TABLE_NAME, CCSA.CHARACTER_SET_NAME " + "FROM INFORMATION_SCHEMA.TABLES " + "JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA" + " ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME WHERE TABLES.TABLE_SCHEMA = ? ";
//    String tblSql = """
//            SELECT
//                TABLES.TABLE_NAME,
//                CCSA.CHARACTER_SET_NAME
//            FROM
//                INFORMATION_SCHEMA.TABLES
//                JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME
//            WHERE TABLES.TABLE_SCHEMA = ?
//            """;
    String columnSql_old = "SELECT " + "TABLE_NAME," + "COLUMN_NAME, " + "DATA_TYPE, " + "CHARACTER_SET_NAME, " + "ORDINAL_POSITION, " + "COLUMN_TYPE, " + "DATETIME_PRECISION, " + "COLUMN_KEY " + "FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION";
//    String columnSql = """
//            SELECT
//            TABLE_NAME,
//            COLUMN_NAME,
//            DATA_TYPE,
//            CHARACTER_SET_NAME,
//            ORDINAL_POSITION,
//            COLUMN_TYPE,
//            DATETIME_PRECISION,
//            COLUMN_KEY
//            FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION
//            """;
    String pkSQl_old = "SELECT " + "TABLE_NAME, " + "COLUMN_NAME, " + "ORDINAL_POSITION " + "FROM information_schema.KEY_COLUMN_USAGE " + "WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ? " + "ORDER BY TABLE_NAME, ORDINAL_POSITION";
//    String pkSQl = """
//            SELECT
//            TABLE_NAME,
//            COLUMN_NAME,
//            ORDINAL_POSITION
//            FROM information_schema.KEY_COLUMN_USAGE
//            WHERE CONSTRAINT_NAME = 'PRIMARY' AND TABLE_SCHEMA = ?
//            ORDER BY TABLE_NAME, ORDINAL_POSITION
//            """;

    public SchemaCapture(Connection connection, ServerCaseSensitivity caseSensitivity) {
//        this.druidPooledConnection = connection;
        this.sensitivity = caseSensitivity;
        this.connection = connection;
        try {
            dbPreparedStatement = connection.prepareStatement(dbCaptureQuery_old);

            tablePreparedStatement = connection.prepareStatement(tblSql_old);
            columnPreparedStatement = connection.prepareStatement(columnSql_old);
            pkPreparedStatement = connection.prepareStatement(pkSQl_old);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    //todo
    public void setupDbSQLParam(){
        sqlProperties.setProperty("","");
    }

    public Schema capture() {

        List<DataBase> customDatabaseList = new ArrayList<>();

        List<DataBase> dbList = new ArrayList<>();

        try (ResultSet rs = dbPreparedStatement.executeQuery()) {
            while (rs.next()) {


                String dbName = rs.getString(SCHEMA_NAME.o());
                String dbCharset = rs.getString(DEFAULT_CHARACTER_SET_NAME.o());

                if (IGNORED_DATABASES.contains(dbName)) continue;

                if (check(dbName, dbCharset)) throw new NullPointerException();//TODO 添加 正则匹配
                DataBase dataBase = new DataBase(dbName, dbCharset, this.sensitivity);
                dbList.add(dataBase);


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

    private boolean check(String... args) {
        for (String arg : args) {
            return StringUtils.isBlank(arg);
        }
        return false;
    }

    private String captureDefaultCharset() throws SQLException {
//        LOGGER.debug("Capturing Default Charset");
        try (Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("select @@character_set_server")) {
            rs.next();
            return rs.getString("@@character_set_server");
        }
    }

    public void captureDatabase(DataBase database) {

        HashMap<String, Table> tables = null;

        try {

            tablePreparedStatement.setString(1, database.getName());
//        Sql.prepareInList(tablePreparedStatement, 2, includeTables);
            tables = new HashMap<>();
            try (ResultSet rs = tablePreparedStatement.executeQuery()) {
                while (rs.next()) {

                    String tableName = rs.getString(TABLE_NAME.o());
                    String characterSetName = rs.getString(CHARACTER_SET_NAME.o());

                    Table t = database.buildTable(tableName, characterSetName);
                    tables.put(tableName, t);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        captureTables(database, tables);
    }


    private void captureTables(DataBase database, HashMap<String, Table> tables) {


        try {
            String name = database.getName();
            columnPreparedStatement.setString(1, database.getName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        try (ResultSet resultSet = columnPreparedStatement.executeQuery();) {

            HashMap<String, Integer> pkIndexCounters = new HashMap<>();
            for (String tableName : tables.keySet()) {
                pkIndexCounters.put(tableName, 0);
            }

            while (resultSet.next()) {
                String[] enumValues = null;

                String currentTableName = resultSet.getString(TABLE_NAME.o());
//                String tableName = resultSet.getString("TABLE_NAME");

//                database.buildTable("tableName", "characterSetName");

                Table table = null;
                if (!tables.containsKey(currentTableName)) {
                    table = tables.computeIfAbsent(currentTableName, k -> database.buildTable(currentTableName, database.getCharset()));
                } else {
                    table = tables.get(currentTableName);
                }
                String colName = resultSet.getString(COLUMN_NAME.o());
                String colType = resultSet.getString(DATA_TYPE.o());
                String colEnc = resultSet.getString(CHARACTER_SET_NAME.o());
                short colPos = (short) (resultSet.getInt(ORDINAL_POSITION.o()) - 1);
                boolean colSigned = !resultSet.getString(COLUMN_TYPE.o()).matches(".* unsigned$");
                long columnLength;

                columnLength = resultSet.getLong(DATETIME_PRECISION.o());


                if (colType.equals("enum") || colType.equals("set")) {
                    String expandedType = resultSet.getString(COLUMN_TYPE.o());
                    enumValues = extractEnumValues(expandedType);
                }
                table.addColumn(ColumnDef.build(colName, colEnc, colType, colPos, colSigned, enumValues, columnLength));

                    // 检查该字段是否为主键 并更新表对象的主键数量。
                    if (resultSet.getString(COLUMN_KEY.o()).equals(PRI.o())){
                        pkIndexCounters.put(currentTableName, pkIndexCounters.get(currentTableName) + 1);
                        table.pkIndex = pkIndexCounters.get(currentTableName);
                    }
            }

        } catch (Exception e) {
            System.out.println(e);
        }
        captureTablesPK(database, tables);
    }

    private void captureTablesPK(DataBase database, HashMap<String, Table> tables) {

        try {
            pkPreparedStatement.setString(1, database.getName());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, ArrayList<String>> tpkMap = tables.keySet().stream().collect(Collectors.toMap(v -> v, v -> new ArrayList<>()));

//        HashMap<String, ArrayList<String>> tablePKMap = new HashMap<>();

        try (ResultSet rs = pkPreparedStatement.executeQuery()) {
//            for (String tableName : tables.keySet()) {
//                tablePKMap.put(tableName, new ArrayList<>());
//            }
            while (rs.next()) {
                int ordinalPosition = rs.getInt(ORDINAL_POSITION.o());
                String tableName = rs.getString(TABLE_NAME.o());
                String columnName = rs.getString(COLUMN_NAME.o());

                ArrayList<String> pkList = tpkMap.get(tableName);
                if (pkList != null)
                    pkList.add(ordinalPosition - 1, columnName);
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            String key = entry.getKey();
            Table table = entry.getValue();

            table.setPKList(tpkMap.get(key));
        }
    }

    public String[] extractEnumValues(String expandedType) {
        Matcher matcher = Pattern.compile("(enum|set)\\((.*)\\)").matcher(expandedType);
        matcher.matches(); // why do you tease me so.
        String enumValues = matcher.group(2);

        if (!(enumValues.endsWith(","))) {
            enumValues += ",";
        }

        String regex = "('.*?'),";
        Pattern pattern = Pattern.compile(regex);
        Matcher enumMatcher = pattern.matcher(enumValues);

        List<String> result = new ArrayList<>();
        while (enumMatcher.find()) {
            String value = enumMatcher.group(0);
            if (value.startsWith("'")) value = value.substring(1);
            if (value.endsWith("',")) {
                value = value.substring(0, value.length() - 2);
            }
            result.add(value);
        }
        return result.toArray(new String[0]);
    }

    public static class Sql {
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
