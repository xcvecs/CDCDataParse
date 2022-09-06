package top.byteinfo.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import top.byteinfo.schema.Table;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Collectors;

public class AtomEvent {
    private LinkedHashMap<Integer, Event> relationEvents;
    private List<Event> relationNativeEvents;

//    private Table currentTable;
    public AtomEvent() {
        this(5);
    }

    public AtomEvent(Integer cap) {
        this(new LinkedHashMap<>(1<<cap));
    }

    public AtomEvent(LinkedHashMap<Integer, Event> relationEvents) {
        this.relationEvents = relationEvents;
        this.relationNativeEvents = new ArrayList<>();
    }

    public void add(Event event) {

        relationNativeEvents.add(event);

    }

    public void clean() {
        relationEvents.clear();
    }

    private void check() {

    }

    protected LinkedHashMap<Integer, Event> getRelationEvents() {
        for (int i = 0; i < relationNativeEvents.size(); i++) {
            relationEvents.put(i,relationNativeEvents.get(i));
        }
        return relationEvents;
    }

    private Integer generateId(Event event) {
        switch (event.getHeader().getEventType()) {
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
