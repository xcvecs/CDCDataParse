package top.byteinfo.iter.schema;

import top.byteinfo.source.maxwell.schema.columndef.ColumnDef;

import java.util.ArrayList;
import java.util.List;

public class DataBase {

    private final String name;
    private final List<Table> tableList;
    private String charset;
    private ServerCaseSensitivity sensitivity;

    public DataBase(String name, String charset, List<Table> tableList, ServerCaseSensitivity sensitivity) {
        this.name = name;
        this.tableList = tableList;
        this.charset = charset;
        this.sensitivity = sensitivity;
    }

    public DataBase(String name, String charset, ServerCaseSensitivity sensitivity) {
        this(name, charset, null, sensitivity);
    }

    public String getName() {
        return name;
    }

    public Table buildTable(String tableName, String characterSetName) {
        return buildTable(tableName, charset, new ArrayList<ColumnDef>(), null);

    }

    public Table buildTable(String tableName, String charset, List<ColumnDef> list, List<String> pks) {
        if (charset == null)
            charset = getCharset(); // inherit database's default charset

//        todo
//        if ( sensitivity == ServerCaseSensitivity.CONVERT_TO_LOWER ){
//            String  dbName = name.toLowerCase();
//        }

        Table t = new Table(this.name, tableName, charset, list, pks);

        this.tableList.add(t);
        return t;
    }
    public String getCharset() {
        if ( charset == null ) {
            // TODO: return server-default charset
            return "";
        } else {
            return charset;
        }
    }

}