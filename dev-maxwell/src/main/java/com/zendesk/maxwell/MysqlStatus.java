package com.zendesk.maxwell;

import com.zendesk.maxwell.constant.CaseSensitivity;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;

public class MysqlStatus {
    private static final Logger log = Logger.getLogger(MysqlStatus.class.getName());
    public static CaseSensitivity captureCaseSensitivity(Connection c) {

        try {
            final int value;
            try (Statement stmt = c.createStatement();
                 ResultSet rs = stmt.executeQuery("select @@lower_case_table_names")) {
                if (!rs.next())
                    throw new RuntimeException("Could not retrieve @@lower_case_table_names!");
                value = rs.getInt(1);
            }

            switch (value) {
                case 0:
                    return CaseSensitivity.CASE_SENSITIVE;
                case 1:
                    return CaseSensitivity.CONVERT_TO_LOWER;
                case 2:
                    return CaseSensitivity.CONVERT_ON_COMPARE;
                default:
                    throw new RuntimeException("Unknown value for @@lower_case_table_names: " + value);
            }
        }catch (Exception e){
            log.info(e.toString());
            return null;
        }

    }

}
