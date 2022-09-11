package com.zendesk.maxwell;

import java.util.HashMap;
import java.util.List;

public class Filter {
    List<String> tableList;

    HashMap<String,List<String>> databases;

    public static boolean includes(Filter filter, String databaseName, String table) {

      return   filter.isTableBlacklisted(databaseName,table);
    }


    public boolean isTableBlacklisted(String database, String table) {
        List<String> stringList = databases.get(database);

        return stringList.stream().anyMatch(s -> s.equals(table));
    }


    public boolean isDatabaseBlacklisted(String database) {
      return   databases.keySet().contains(database);
    }
}
