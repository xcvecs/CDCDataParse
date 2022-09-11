package top.byteinfo.schema;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import java.util.List;
import java.util.Objects;

public class TableCache {

    private Table table;

    public Table getTable() {
        if (table==null){

        }
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public void clear() {

        this.table = null;
    }

    public void put(String valueOf, Table table) {

    }

    public boolean processEvent(Schema schema, TableMapEventData eventData) {
        List<Table> tableList = schema.getDbMap().get(eventData.getDatabase()).getTableList();
        for (Table table : tableList) {
            if (Objects.equals(table.name, eventData.getTable())){
                setTable(table);
                return true;
            }
        }
        return false;
    }

    public void processEvent(Schema schema, Object filter, long tableId, String database, String table) {

    }
}
