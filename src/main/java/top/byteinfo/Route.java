package top.byteinfo;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;

import java.util.regex.Pattern;

public class Route {
    private boolean skipTable;
    private volatile Pattern databaseNamePattern;
    private volatile Pattern tableNamePattern;

    public void getDataEvent(Event event){

        EventHeaderV4  header = event.getHeader();


        switch(header.getEventType()){
            case TABLE_MAP:
                TableMapEventData data = event.getData();
                // Should we skip this table? Yes if we've specified a DB or table name pattern and they don't match todo regular expression
                skipTable = (databaseNamePattern != null && !databaseNamePattern.matcher(data.getDatabase()).matches())
                        || (tableNamePattern != null && !tableNamePattern.matcher(data.getTable()).matches());

            case QUERY:

            case XID:


            case WRITE_ROWS:
            case EXT_WRITE_ROWS:
            case PRE_GA_WRITE_ROWS:
            case UPDATE_ROWS:
            case EXT_UPDATE_ROWS:
            case PRE_GA_UPDATE_ROWS:
            case DELETE_ROWS:
            case EXT_DELETE_ROWS:
            case PRE_GA_DELETE_ROWS:

            case ROTATE:

            case GTID:


            default:
                break;


        }








    }
}
