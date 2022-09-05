package top.byteinfo.iter.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import top.byteinfo.iter.binlog.DataEvent.DataEventType;
import top.byteinfo.iter.producer.ChangedEvent;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.github.shyiko.mysql.binlog.event.EventType.ANONYMOUS_GTID;
import static com.github.shyiko.mysql.binlog.event.EventType.QUERY;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;
import static com.github.shyiko.mysql.binlog.event.EventType.UNKNOWN;
import static com.github.shyiko.mysql.binlog.event.EventType.XID;
import static top.byteinfo.iter.binlog.DataEvent.DataEventType.Boundary;
import static top.byteinfo.iter.binlog.DataEvent.DataEventType.BoundaryDDL;
import static top.byteinfo.iter.binlog.DataEvent.DataEventType.BoundaryDML;
import static top.byteinfo.iter.binlog.DataEvent.DataEventType.DDL;
import static top.byteinfo.iter.binlog.DataEvent.DataEventType.DML;


public class NativeDataEvent {
    private final List<Event> events;
    private String type;
    private Long xid;
    private List<Long> ts;
    private Long xoffset = 0L;
    private Long serverId;
    private Long threadId;
    private Long schemaId;

    private ChangedEvent changedEvent;

    public NativeDataEvent(List<Event> events) {
        this.events = events;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Event> getEvents() {
        return events;
    }

//    public void setEvents(List<Event> events) {
//        this.events = events;
//    }

    public Long getXid() {
        return xid;
    }

    public void setXid(Long xid) {
        this.xid = xid;
    }

    public List<Long> getTs() {
        return ts;
    }

    public void setTs(List<Long> ts) {
        this.ts = ts;
    }

    public Long getXoffset() {
        return xoffset;
    }

    public void setXoffset(Long xoffset) {
        this.xoffset = xoffset;
    }

    public Long getServerId() {
        return serverId;
    }

    public void setServerId(Long serverId) {
        this.serverId = serverId;
    }

    public Long getThreadId() {
        return threadId;
    }

    public void setThreadId(Long threadId) {
        this.threadId = threadId;
    }

    public Long getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(Long schemaId) {
        this.schemaId = schemaId;
    }

    public ChangedEvent getChangedEvent() {
        return changedEvent;
    }

    public void setChangedEvent(ChangedEvent changedEvent) {
        this.changedEvent = changedEvent;
    }

    public DataEvent removeFirst() {


        switch (events.size()) {
            case 2:
                boolean checkDDL = check(events, DDL);
                if (checkDDL) return new DataEvent(DDL, events);
                break;
            case 5:
                boolean checkDML = check(events, DML);
                if (checkDML) return new DataEvent(DML, events);
                break;
            default:
                 return new DataEvent(Boundary, events);
        }
        if (events.size() == 2) return new DataEvent(BoundaryDDL, events);
        return new DataEvent(BoundaryDML, events);
    }

    /*private DataEvent dataEventRoute() {
        List<EventType> isRowMutation = Arrays.asList(ANONYMOUS_GTID, QUERY, TABLE_MAP, XID);
        List<EventType> eventTypes = events.stream().map(event -> event.getHeader().getEventType()).collect(Collectors.toList());

        List<Event> queryList = events.stream().filter(event -> event.getHeader().getEventType().equals(QUERY)).collect(Collectors.toList());
        List<Event> beginQueryList = queryList.stream().filter(event -> ((QueryEventData) event.getData()).getSql().toLowerCase().startsWith("begin")).collect(Collectors.toList());
        if (1 == queryList.size() && queryList.size() == beginQueryList.size())
            return new DataEvent(BoundaryDML, events);


        return new DataEvent(Boundary, events);

    }*/

    public boolean check(List<Event> nativeList, DataEventType dataEventType) {

        List<EventType> nativeEventTypes = nativeList.stream().map(event -> event.getHeader().getEventType()).collect(Collectors.toList());
        List<EventType> eventTypes;
        switch (dataEventType) {

            case DDL:
                System.out.println();//todo
                eventTypes = Arrays.asList(ANONYMOUS_GTID, QUERY);
                break;
            case DML:
                System.out.println("");//todo
                List<EventType> types = Arrays.asList(ANONYMOUS_GTID, QUERY, TABLE_MAP, null, XID);
                EventType eventType = nativeEventTypes.stream().filter(EventType::isRowMutation).findAny().orElse(
                        UNKNOWN
                );
                eventTypes = types.stream().map(type -> {
                    if (type == null) return eventType;
                    return type;
                }).collect(Collectors.toList());
                break;
            default:
                eventTypes = Arrays.asList(UNKNOWN, UNKNOWN);
        }


        if (nativeList.size() != eventTypes.size()) return false;
        if (dataEventType.equals(DML)) {
            EventType nativeEventType = nativeList.get(3).getHeader().getEventType();

            /*EventType nativeEventType = nativeList.stream().filter(event -> EventType.isRowMutation(event.getHeader().getEventType())).findAny().orElseGet(() -> {
                EventHeaderV4 header = new EventHeaderV4();
                header.setEventType(EventType.XID);
                return new Event(new EventHeaderV4(), new XidEventData());
            }).getHeader().getEventType();// EventType.isRowMutation*/

            /*eventTypes = dataEventType.getDesp().stream().map(eventType -> {
                if (eventType == null && EventType.isRowMutation(nativeEventType)) return nativeEventType;
                return eventType;
            }).collect(Collectors.toList());*/
//            dataEventType.setDesp(dataEventTypeDesp);
        }


        for (int i = 0; i < nativeEventTypes.size(); i++) {
            if (!nativeEventTypes.get(i).equals(eventTypes.get(i))) return false;
        }
        return true;
    }
//

    public boolean isEmpty() {
        return false;
    }
}
