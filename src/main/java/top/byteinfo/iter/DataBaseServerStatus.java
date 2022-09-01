package top.byteinfo.iter;

import java.sql.Connection;

public class DataBaseServerStatus {

    private final Connection connection;

    public DataBaseServerStatus(Connection connection) {
        this.connection = connection;
    }
    private String baseSqlStatement(String variableName) {
        return "SHOW VARIABLES LIKE '" + variableName + "'";
    }


}
