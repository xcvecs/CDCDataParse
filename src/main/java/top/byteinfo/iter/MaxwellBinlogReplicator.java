package top.byteinfo.iter;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.byteinfo.iter.binlog.DataEvent;
import top.byteinfo.iter.execption.BinLogFileException;
import top.byteinfo.iter.execption.BoundaryConditionException;
import top.byteinfo.iter.schema.Schema;
import top.byteinfo.iter.schema.SchemaCapture;
import top.byteinfo.iter.schema.Table;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static com.github.shyiko.mysql.binlog.event.EventType.*;
import static top.byteinfo.iter.binlog.DataEvent.DataEventType.*;
import static top.byteinfo.iter.util.CheckParam.dataEventPreCheck;

public class MaxwellBinlogReplicator implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MaxwellBinlogReplicator.class);
    private final LinkedBlockingDeque<Event> deque;
    private final LinkedBlockingDeque<DataEvent> dataEvents;
    private final LinkedList<Event> rowEventList;
    private Schema schema;
    private SchemaCapture schemaCapture;


    private LinkedHashMap<String, Table> tableCache;

    public MaxwellBinlogReplicator(
            LinkedBlockingDeque<Event> deque,
            Schema schema,
            SchemaCapture schemaCapture,
            LinkedList<Event> rowEvent,
            LinkedHashMap<String, Table> tableCache) {

        this.deque = deque;
        this.schema = schema;
        this.schemaCapture = schemaCapture;
        this.rowEventList = rowEvent;
        this.tableCache = tableCache;
        this.dataEvents = new LinkedBlockingDeque<>();
    }

    public MaxwellBinlogReplicator(LinkedBlockingDeque<Event> linkedBlockingDeque, Schema schema, SchemaCapture schemaCapture) {
        this(linkedBlockingDeque, schema, schemaCapture, new LinkedList<>(), new LinkedHashMap<>());
    }


    protected Event pollEvent() {
        try {
            return deque.poll(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("...");
            return null;
        }
    }


    /**
     * 1.挨个读取 序列。
     * 2.
     */
    public void route() {
        boolean eventFormat = false;
        boolean binlogValid = false;
        boolean parseFlag = false;


        int routCount = 0;
        /**
         * todo
         */
        LinkedList<Event> eventTemp = new LinkedList<>();
        log.debug("event route starting");
        while (true) {
            if (!eventFormat) {
                log.debug("");
                Event event = ensureEvent();
                EventType eventType = event.getHeader().getEventType();
                if (eventType.equals(ROTATE)) {
                    binlogValid = true;
                }
                Event event1 = pollEvent();
                if (event1.getHeader().getEventType().equals(FORMAT_DESCRIPTION)) {
                    parseFlag = true;
                }

                if (binlogValid && parseFlag) {
                    eventFormat = true;
                } else {
                    log.error("binlog file error");
                    throw new BinLogFileException("");
                }
            }
            if (eventTemp.size() != 0 && routCount == 0) {
                //check Argument
                dataEventPreCheck(eventTemp);
                switch (generateType(eventTemp.size())) {
                    case DDL:
                        DataEvent ddlDataEvent = new DataEvent(DDL, eventTemp);
                        dataEvents.add(ddlDataEvent);
                        break;
                    case DML:
                        DataEvent dmlDataEvent = new DataEvent(DML, eventTemp);
                        dataEvents.add(dmlDataEvent);
                        break;
                    default:
                        break;
                }
            }
            Event event = ensureEvent();
            EventType eventType = event.getHeader().getEventType();


            if (eventType.equals(ANONYMOUS_GTID)) {
                if (routCount != 0) throw new BoundaryConditionException();
                eventTemp.add(event);
                routCount++;
            }


            if (eventType.equals(QUERY)) {
                eventTemp.add(event);
                QueryEventData eventData = event.getData();//read .....
                if (!eventData.getSql().equals("BEGIN")) {
                    routCount = 0;
                }
            }
            if (eventType.equals(XID)) {
                eventTemp.add(event);
                routCount = 0;
                continue;
            }

            if (routCount == 0) continue;
            eventTemp.add(event);


        }

    }

    private Event ensureEvent() {
        // TODO
        Event event = pollEvent();
        int timeCount = 0;
        while (event == null) {
            timeCount += timeCount < 9 ? 3 : 0;
            try {
                Thread.sleep(1L << timeCount);
            } catch (InterruptedException e) {
                log.warn(e.getMessage());
            }
            event = pollEvent();
        }
        return event;
    }

    public void parseEventData() {
        /**
         * event 预处理
         */
        log.debug("异步预处理数据 0");
        Executors.newSingleThreadExecutor().submit(this::route);
        log.debug("异步预处理数据 1");

        int count = 0;
        while (true) {
            int size = deque.size();
            int sizes = dataEvents.size();

            count += count >= 13 ? 0 : 3;
            try {
                Thread.sleep(1L << count);
            } catch (InterruptedException e) {
                log.warn(e.getMessage());
            }
        }
    }

    public boolean binlogCheck() {

//        DataEvent dataEvent = dataEvents.p;

        return true;
    }


    @Override
    public void run() {
        parseEventData();

    }


    public void source() {
            /*public void work() {

        Event row = getRawEvent();

    }*/

    /*public Event getRawEvent() {

        Event event = null;


        while (true) {
            event = pollEvent();

            if (event == null) {
                event = ensureReplicatorThread();//blocking
            }




            switch (event.getHeader().getEventType()) {
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:

                    break;
                case TABLE_MAP:
                    TableMapEventData tableMapEventData = event.getData();

                    processEvent(tableMapEventData);
                    System.out.println("ok");
                    break;
                case QUERY:
                    QueryEventData eventData = event.getData();
                    if (eventData.getSql().equals(BEGIN.name())) {
                    } else {
                        processQueryRawEvent(event);
                    }


                    System.out.println();
                    break;
                case ROTATE:
                    System.out.println("");
                    break;
                default:
                    break;

            }

        }
    }*/

    /*private Event ensureReplicatorThread() {
        // TODO
        Event event = null;
        int timeCount = 0;
        while (event == null) {
            timeCount += timeCount < 15 ? 1 : 0;
            try {
                Thread.sleep(1L << timeCount);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            event = pollEvent();
        }
        return event;
    }*/

   /* private void processEvent(TableMapEventData tableMapEventData) {


    }*/

    /*private LinkedList<Event> getTransactionRowEvents(Event event) {

        while (true) {
//            event = pollEvent();
            if (event == null) {
                event = ensureReplicatorThread();//blocking
            }

            EventType eventType = event.getHeader().getEventType();
            if (eventType == EventType.XID) {//
//                return rowEvent;
            }

            switch (eventType) {
                case TABLE_MAP:
                    System.out.println();
//                    TableMapEventData data = event.tableMapData();
//                    tableCache.processEvent(getSchema(), this.filter, data.getTableId(), data.getDatabase(), data.getTable());
                    break;
                case WRITE_ROWS:
                case UPDATE_ROWS:
                case DELETE_ROWS:
                case EXT_WRITE_ROWS:
                case EXT_UPDATE_ROWS:
                case EXT_DELETE_ROWS:
//                    rowEvent.add(event);
                    break;


                case QUERY:
                    QueryEventData qe = event.getData();
                    String sql = qe.getSql();
                    String upperCaseSql = sql.toUpperCase();
                    if (upperCaseSql.startsWith("SAVEPOINT")) {
//                        LOGGER.debug("Ignoring SAVEPOINT in transaction: {}", qe);
//                    } else if ( createTablePattern.matcher(sql).find() ) {
//                        // CREATE TABLE `foo` SELECT * FROM `bar` will put a CREATE TABLE
//                        // inside a transaction.  Note that this could, in rare cases, lead
//                        // to us starting on a WRITE_ROWS event -- we sync the schema position somewhere
//                        // kinda unsafe.
//                        processQueryEvent(event);
//                    } else if (upperCaseSql.startsWith("INSERT INTO MYSQL.RDS_") || upperCaseSql.startsWith("DELETE FROM MYSQL.RDS_")) {
//                        // RDS heartbeat events take the following form:
//                        // INSERT INTO mysql.rds_heartbeat2(id, value) values (1,1483041015005) ON DUPLICATE KEY UPDATE value = 1483041015005
//
//                        // Other RDS internal events like below:
//                        // INSERT INTO mysql.rds_sysinfo(name, value) values ('innodb_txn_key','Thu Nov 15 10:30:07 UTC 2018')
//                        // DELETE FROM mysql.rds_sysinfo where name = 'innodb_txn_key'

                        // We don't need to process them, just ignore
                    } else if (upperCaseSql.startsWith("DROP TEMPORARY TABLE")) {
                        // Ignore temporary table drop statements inside transactions
                    } else if (upperCaseSql.startsWith("# DUMMY EVENT")) {
                        // MariaDB injected event
                    } else {
//                        LOGGER.warn("Unhandled QueryEvent @ {} inside transaction: {}", event.getPosition().fullPosition(), qe);
                    }
                    break;
            }


        }

    }*/


    /*private void processQueryRawEvent(Event event) {

//        rowEvent.add(event);
    }*/


    /*public void handlerEvents(Collection<Event> collection) {

        Iterator<Event> eventIterator = collection.iterator();

        while (eventIterator.hasNext()) {

            Event event = eventIterator.next();
            System.out.println(event);
//           if (!eventHandler(event)){
//               throw new RuntimeException("error ");
//           }


        }
    }*/

    /*public boolean eventHandler(Event event) {

        ObjectMapper objectMapper = new ObjectMapper();


        if (event.getHeader().getEventType().equals(TABLE_MAP)) {
            EventData eventData = event.getData();
            TableMapEventData tableMapEventData = (TableMapEventData) eventData;

            byte[] columnTypes = tableMapEventData.getColumnTypes();

            List<Byte> columnTypesList = new LinkedList<>();
            for (byte columnType : columnTypes) {
                columnTypesList.add(columnType);
            }

            AtomicInteger count = new AtomicInteger(0);
            System.out.println(columnTypesList);
            Map<Integer, Object> collect = columnTypesList.stream().collect(Collectors.toMap(i -> count.getAndIncrement(), Byte::byteValue));

            for (Map.Entry<Integer, Object> integerObjectEntry : collect.entrySet()) {
                System.out.println(integerObjectEntry.getKey() + ":" + integerObjectEntry.getValue());

            }
//            Class<Columntypes> columntypesClass = Columntypes.class;
//            Field[] declaredFields = columntypesClass.getDeclaredFields();

        }

        if (event.getHeader().getEventType().equals(EXT_WRITE_ROWS)) {

            EventData eventData = event.getData();

            UpdateRowsEventData updateRowsEventData = (UpdateRowsEventData) eventData;

            List<Map.Entry<Serializable[], Serializable[]>> entries = updateRowsEventData.getRows();

            LinkedList<Serializable[]> keySerializable = entries.stream().map(Map.Entry::getKey).collect(Collectors.toCollection(LinkedList::new));
            LinkedList<Serializable[]> ValueSerializable = entries.stream().map(Map.Entry::getValue).collect(Collectors.toCollection(LinkedList::new));


            List list = new ArrayList<>();
        }
        return true;
    }*/
    }
}
