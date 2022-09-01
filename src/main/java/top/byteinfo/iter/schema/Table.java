package top.byteinfo.iter.schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import top.byteinfo.iter.schema.columndef.ColumnDef;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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

    public TableColumnList getColumns() {
        return columns;
    }

    public Table(String database, String tableName, String charset, List<ColumnDef> list, List<String> pks) {

        this.database=database;
        this.name=tableName;
        this.charset=charset;

        this.setColumnList(list);

        if ( pks == null )
            pks = new ArrayList<String>();

        this.setPKList(pks);

    }

    private void setColumnList(List<ColumnDef> list) {
        this.columns = new TableColumnList(list);
    }

    public void addColumn(ColumnDef definition) {
        columns.add(columns.size(), definition);
        // todo
        this.columns.columnNames();
    }

    @JsonProperty("primary-key")
    public synchronized void setPKList(List<String> pkColumnNames) {
        this.pkColumnNames = pkColumnNames.stream().map((n) -> n.intern()).collect(Collectors.toList());
        this.normalizedPKColumnNames = null;
    }
}
