package top.byteinfo.iter.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import top.byteinfo.source.maxwell.schema.TableColumnList;
import top.byteinfo.source.maxwell.schema.columndef.ColumnDef;

import java.util.List;

public class Table {

    public String database;
    @JsonProperty("table")
    public String name;
    private TableColumnList columns;
    public String charset;
    private List<String> pkColumnNames;
    private List<String> normalizedPKColumnNames;

    @JsonIgnore
    public int pkIndex;


    public Table(String database, String tableName, String charset, List<ColumnDef> list, List<String> pks) {

    }
}
