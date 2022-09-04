package top.byteinfo.iter.binlog;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.LinkedHashMap;

public class AtomEvent {
    LinkedHashMap<Integer, Event>  relationEvent;

    public void add(Event event){
        this.add(event,generateId(event));
    }
    public void add(Event event,Integer relationId){
        relationEvent.put(relationId,event);
    }

    public void clean(){
        relationEvent.clear();
    }
    private void check(){

    }

    public Integer generateId(Event event){
        switch (event.getHeader().getEventType()){
            case ROTATE:
            case FORMAT_DESCRIPTION:
                return 0;
            case ANONYMOUS_GTID:
                return 1;
            case QUERY:
                return 2;
            case TABLE_MAP:
                return 3;
            case WRITE_ROWS:
            case DELETE_ROWS:
            case UPDATE_ROWS:
            case EXT_WRITE_ROWS:
            case EXT_DELETE_ROWS:
            case EXT_UPDATE_ROWS:
                return 4;
            case XID:
                return 5;
            default:
                return -1;
        }
    }
}
