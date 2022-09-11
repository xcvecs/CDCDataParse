package top.byteinfo.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class Schema {
    private final String schemaId;
    private final Map<String, DataBase> dbMap;
    private final String charset;
    private final ServerCaseSensitivity sensitivity;

    public Schema(Map<String, DataBase> dbMap, String charset, ServerCaseSensitivity sensitivity) {
        long l = new Random().longs(1 << 20, 2 << 25).limit(1).sum();
        this.schemaId= String.valueOf(l);
        this.dbMap = dbMap;
        this.charset = charset;
        this.sensitivity = sensitivity;

    }

    public String getSchemaId() {
        return schemaId;
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
