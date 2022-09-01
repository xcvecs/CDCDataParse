package top.byteinfo.iter;

import org.apache.commons.lang3.StringUtils;
import top.byteinfo.source.maxwell.schema.CustomDatabase;
import top.byteinfo.source.maxwell.schema.CustomTable;
import top.byteinfo.source.maxwell.schema.CustomSchema;
import top.byteinfo.source.maxwell.schema.columndef.ColumnDef;

import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CustomSchemaCapture implements AutoCloseable {
    public static final HashSet<String> IGNORED_DATABASES = new HashSet<>(
            Arrays.asList(new String[]{"performance_schema", "information_schema"})
    );
    static final Logger LOGGER = Logger.getLogger(CustomSchemaCapture.class.getName());

    ;

    private final Connection connection;
    private final Set<String> includeDatabases;
    private final Set<String> includeTables;
    private final CaseSensitivity sensitivity;
    private final boolean isMySQLAtLeast56;
    private final PreparedStatement tablePreparedStatement;
    private final PreparedStatement columnPreparedStatement;
    private final PreparedStatement pkPreparedStatement;

    String dbCaptureQuery_old =
            "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA";

    String dbCaptureQuery = """
            SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA
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


    public CustomSchemaCapture(
            Connection connection,
            CaseSensitivity sensitivity
    ) throws SQLException {
        this.connection = connection;
        this.includeDatabases = new HashSet<>();//
        this.includeTables = new HashSet<>();//
        this.sensitivity = sensitivity;//
// todo
//        this.isMySQLAtLeast56 =  checkMysqlVersion();
//        String dateTimePrecision = "";
//        if (isMySQLAtLeast56) {
//            dateTimePrecision = "DATETIME_PRECISION, ";
//        }

//        this.isMySQLAtLeast56 = isMySQLAtLeast56();
        this.isMySQLAtLeast56 = true;
//        String dateTimePrecision = "";
//        if (isMySQLAtLeast56) {
//            dateTimePrecision = "DATETIME_PRECISION, ";
//        }

        if (!includeTables.isEmpty()) {
            tblSql += " AND TABLES.TABLE_NAME IN " + Sql.inListSQL(includeTables.size());
        }
        this.tablePreparedStatement = connection.prepareStatement(tblSql);

        this.columnPreparedStatement = connection.prepareStatement(columnSql);

        this.pkPreparedStatement = connection.prepareStatement(pkSQl);


    }


    private boolean isMySQLAtLeast56() throws SQLException {
        java.sql.DatabaseMetaData meta = connection.getMetaData();
        int major = meta.getDatabaseMajorVersion();
        int minor = meta.getDatabaseMinorVersion();
        return ((major == 5 && minor >= 6) || major > 5);
    }

    public CustomSchema capture() throws SQLException {
//        LOGGER.debug("Capturing schemas...");
        ArrayList<CustomDatabase> customDatabases = new ArrayList<>();

//        String dbCaptureQuery =
//                "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME FROM INFORMATION_SCHEMA.SCHEMATA";

        if (includeDatabases.size() > 0) {
            dbCaptureQuery +=
                    " WHERE SCHEMA_NAME IN " + Sql.inListSQL(includeDatabases.size());
        }
        dbCaptureQuery += " ORDER BY SCHEMA_NAME";

        try (PreparedStatement statement = connection.prepareStatement(dbCaptureQuery)) {
            Sql.prepareInList(statement, 1, includeDatabases);

            try (ResultSet rs = statement.executeQuery()) {
                while (rs.next()) {
                    String dbName = rs.getString("SCHEMA_NAME");
                    String charset = rs.getString("DEFAULT_CHARACTER_SET_NAME");

                    if (IGNORED_DATABASES.contains(dbName))
                        continue;

                    CustomDatabase db = new CustomDatabase(dbName, charset);
                    customDatabases.add(db);
                }
            }
        }

        int size = customDatabases.size();
//        LOGGER.debug("Starting schema capture of {} databases...", size);
        int counter = 1;
        for (CustomDatabase db : customDatabases) {
//            LOGGER.debug("{}/{} Capturing {}...", counter, size, db.getName());
            captureDatabase(db);
            counter++;
        }
//        LOGGER.debug("{} database schemas captured!", size);

        return new CustomSchema(customDatabases, captureDefaultCharset(), this.sensitivity);
    }

    private String captureDefaultCharset() throws SQLException {
//        LOGGER.debug("Capturing Default Charset");
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("select @@character_set_server")) {
            rs.next();
            return rs.getString("@@character_set_server");
        }
    }

    private void captureDatabase(CustomDatabase db) throws SQLException {

        tablePreparedStatement.setString(1, db.getName());
        Sql.prepareInList(tablePreparedStatement, 2, includeTables);

        HashMap<String, CustomTable> customTables = new HashMap<>();
        try (ResultSet rs = tablePreparedStatement.executeQuery()) {
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String characterSetName = rs.getString("CHARACTER_SET_NAME");
                CustomTable t = db.buildTable(tableName, characterSetName);
                customTables.put(tableName, t);
            }
        }

        captureTables(db, customTables);
    }

    private void captureTables(CustomDatabase db, HashMap<String, CustomTable> customTables) throws SQLException {
        columnPreparedStatement.setString(1, db.getName());

        try (ResultSet r = columnPreparedStatement.executeQuery()) {

            HashMap<String, Integer> pkIndexCounters = new HashMap<>();
            for (String tableName : customTables.keySet()) {
                pkIndexCounters.put(tableName, 0);
            }

            while (r.next()) {
                String[] enumValues = null;
                String tableName = r.getString("TABLE_NAME");

                if (customTables.containsKey(tableName)) {
                    CustomTable t = customTables.get(tableName);
                    String colName = r.getString("COLUMN_NAME");
                    String colType = r.getString("DATA_TYPE");
                    String colEnc = r.getString("CHARACTER_SET_NAME");
                    short colPos = (short) (r.getInt("ORDINAL_POSITION") - 1);
                    boolean colSigned = !r.getString("COLUMN_TYPE").matches(".* unsigned$");
                    Long columnLength = null;

                    if (isMySQLAtLeast56)
                        columnLength = r.getLong("DATETIME_PRECISION");

                    if (r.getString("COLUMN_KEY").equals("PRI"))
                        t.pkIndex = pkIndexCounters.get(tableName);

                    if (colType.equals("enum") || colType.equals("set")) {
                        String expandedType = r.getString("COLUMN_TYPE");

                        enumValues = extractEnumValues(expandedType);
                    }

                    t.addColumn(ColumnDef.build(colName, colEnc, colType, colPos, colSigned, enumValues, columnLength));

                    pkIndexCounters.put(tableName, pkIndexCounters.get(tableName) + 1);
                }
            }
        }

        captureTablesPK(db, customTables);
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
            if (value.startsWith("'"))
                value = value.substring(1);
            if (value.endsWith("',")) {
                value = value.substring(0, value.length() - 2);
            }
            result.add(value);
        }
        return result.toArray(new String[0]);
    }

    private void captureTablesPK(CustomDatabase db, HashMap<String, CustomTable> customTables) throws SQLException {
        pkPreparedStatement.setString(1, db.getName());

        HashMap<String, ArrayList<String>> tablePKMap = new HashMap<>();

        try (ResultSet rs = pkPreparedStatement.executeQuery()) {
            for (String tableName : customTables.keySet()) {
                tablePKMap.put(tableName, new ArrayList<>());
            }

            while (rs.next()) {
                int ordinalPosition = rs.getInt("ORDINAL_POSITION");
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");

                ArrayList<String> pkList = tablePKMap.get(tableName);
                if (pkList != null)
                    pkList.add(ordinalPosition - 1, columnName);
            }
        }

        for (Map.Entry<String, CustomTable> entry : customTables.entrySet()) {
            String key = entry.getKey();
            CustomTable customTable = entry.getValue();

            customTable.setPKList(tablePKMap.get(key));
        }
    }

    @Override
    public void close() throws SQLException {
        try (PreparedStatement p1 = tablePreparedStatement;
             PreparedStatement p2 = columnPreparedStatement;
             PreparedStatement p3 = pkPreparedStatement) {
            // auto-close shared prepared statements
        }
    }

    public enum CaseSensitivity {CASE_SENSITIVE, CONVERT_TO_LOWER, CONVERT_ON_COMPARE}

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
