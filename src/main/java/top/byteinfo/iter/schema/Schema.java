package top.byteinfo.iter.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Schema {
    private final Map<String, DataBase> dbMap;
    private final String charset;
    private final ServerCaseSensitivity sensitivity;

    public Schema(Map<String, DataBase> dbMap, String charset, ServerCaseSensitivity sensitivity) {
        this.dbMap = dbMap;
        this.charset = charset;
        this.sensitivity = sensitivity;

    }

    public Map<String, DataBase> getDbMap() {
        return dbMap;
    }

    public String getCharset() {
        return charset;
    }

    public ServerCaseSensitivity getSensitivity() {
        return sensitivity;
    }

    public List<String> getDatabaseNames() {
       return new ArrayList<>(getDbMap().keySet());
    }
}
