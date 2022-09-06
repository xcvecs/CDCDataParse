package top.byteinfo.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;

import top.byteinfo.Jackson;
import top.byteinfo.producer.ChangedEvent;

import java.util.*;
import java.util.stream.Collectors;

import static com.github.shyiko.mysql.binlog.event.EventType.*;
import static top.byteinfo.binlog.DataEvent.DataEventType.*;


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

    public NativeDataEvent() {
        this(new ArrayList<>());
    }

    public NativeDataEvent(List<Event> events) {
        this.events = events;
    }

    public NativeDataEvent(AtomEvent atomEvent) {
        LinkedHashMap<Integer, Event> relationEvents = atomEvent.getRelationEvents();

        this.events = new ArrayList<>(relationEvents.values());

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

        List<Event> atomEvents = new ArrayList<>(events);

        switch (events.size()) {
            case 2:
                boolean checkDDL = check(events, DDL);
                if (checkDDL) return new DataEvent(DDL, atomEvents);
                break;
            case 5:
                boolean checkDML = check(events, DML);
                if (checkDML) return new DataEvent(DML, atomEvents);
                break;
            default:
                return new DataEvent(Boundary, atomEvents);
        }
        if (events.size() == 2) return new DataEvent(BoundaryDDL, atomEvents);

        return new DataEvent(BoundaryDML, atomEvents);
    }



    public boolean check(List<Event> nativeList, DataEvent.DataEventType dataEventType) {

        List<EventType> nativeEventTypes = nativeList.stream().map(event -> event.getHeader().getEventType()).collect(Collectors.toList());
        List<EventType> eventTypes;
        switch (dataEventType) {

            case DDL:
                //todo
                eventTypes = Arrays.asList(ANONYMOUS_GTID, QUERY);
                break;
            case DML:
                //todo
                List<EventType> types = Arrays.asList(ANONYMOUS_GTID, QUERY, TABLE_MAP, null, XID);
                EventType nativeEventType = nativeEventTypes.stream().filter(EventType::isRowMutation).findAny().orElse(
                        UNKNOWN
                );
                eventTypes = types.stream().map(type -> {
                    if (type == null) return nativeEventType;
                    return type;
                }).collect(Collectors.toList());
                break;
            default:
                eventTypes = Arrays.asList(UNKNOWN, UNKNOWN);
        }
        if (nativeList.size() != eventTypes.size()) return false;
        for (int i = 0; i < nativeEventTypes.size(); i++) {
            if (!nativeEventTypes.get(i).equals(eventTypes.get(i))) return false;
        }
        return true;
    }

    public boolean generated() {
        return !isEmpty();
    }

    public boolean isEmpty() {

        return events.isEmpty();
    }

    public void clear() {
        events.clear();
    }

    public void generate(AtomEvent atomEvent) {
//        if (generated())
        LinkedHashMap<Integer, Event> relationEvents = atomEvent.getRelationEvents();
        this.events.addAll(relationEvents.values());
    }

    @Override
    public String toString() {
        return Jackson.toString(this);
    }
}
