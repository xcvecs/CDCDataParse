package top.byteinfo.iter.schema;

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


}
