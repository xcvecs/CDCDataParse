package top.byteinfo.iter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.shyiko.mysql.binlog.event.*;
import lombok.SneakyThrows;
import top.byteinfo.source.maxwell.schema.Schema;
import top.byteinfo.source.maxwell.schema.Table;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;
import static top.byteinfo.iter.GlobalConstant.BEGIN;

public class MaxwellBinlogReplicator implements Runnable{

    private Schema schema;
   private CustomSchemaCapture schemaCapture;
    private final LinkedBlockingDeque<Event> deque;
    private LinkedList<Event> rowEvent;

    private LinkedHashMap<String, Table> tableCache;
    public MaxwellBinlogReplicator(
        LinkedBlockingDeque<Event> deque,
        Schema schema,
        CustomSchemaCapture schemaCapture,
        LinkedList<Event> rowEvent,
        LinkedHashMap<String, Table> tableCache) {

        this.deque = deque;
        this.schema=schema;
        this.schemaCapture = schemaCapture;
        this.rowEvent = rowEvent;
        this.tableCache = tableCache;
    }
    public MaxwellBinlogReplicator(LinkedBlockingDeque<Event> linkedBlockingDeque,Schema schema,CustomSchemaCapture schemaCapture) {
        this(linkedBlockingDeque,schema,schemaCapture,new LinkedList<>(),new LinkedHashMap<>());
    }



    public void work()  {
//        RowMap row = null;
//        try {
        Event row = getRawEvent();
//        } catch ( InterruptedException e ) {
//        }
//
//        if ( row == null )
//            return;
//
//        rowCounter.inc();
//        rowMeter.mark();
//
//        if ( scripting != null && !isMaxwellRow(row))
//            scripting.invoke(row);
//
//        processRow(row);
    }

    //handler event
    public Event getRawEvent() {

        Event event = null;


        while (true) {
            event = pollEvent();

            if (event == null) {
                event = ensureReplicatorThread();//blocking
            }


            if (rowEvent != null && !rowEvent.isEmpty()) {
                Event row = rowEvent.removeFirst();
                return row;
            }

            switch (event.getHeader().getEventType()) {
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
//                LOGGER.warn("Started replication stream inside a transaction.  This shouldn't normally happen.");
//                LOGGER.warn("Assuming new transaction at unexpected event:" + event);
//
//                queue.offerFirst(event);
//                rowBuffer = getTransactionRows(event);
                    break;
                case TABLE_MAP:
                    TableMapEventData tableMapEventData = event.getData();

                    processEvent(tableMapEventData);
                    System.out.println("ok");
                    break;
                case QUERY:
                    QueryEventData eventData = event.getData();
                    if (eventData.getSql().equals(BEGIN.name())) {
                        rowEvent = getTransactionRowEvents(event);
                    } else {
                        processQueryRawEvent(event);
                    }


                    System.out.println();
                    break;
                case ROTATE:
//                tableCache.clear();
                    System.out.println("");
                    break;
                default:
                    break;

            }

        }
    }

    private Event ensureReplicatorThread() {
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
    }

    private void processEvent(TableMapEventData tableMapEventData) {


//        Table tableInfo = new Table(tableMapEventData);
//
//        tableCache.put(tableInfo.getTableId(), tableInfo);
    }

    private LinkedList<Event> getTransactionRowEvents(Event event) {
//        Event event1;

        while (true) {
//            event = pollEvent();
            if (event == null) {
                event = ensureReplicatorThread();//blocking
            }

            EventType eventType = event.getHeader().getEventType();
            if (eventType == EventType.XID) {//
                return rowEvent;
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
                    rowEvent.add(event);
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

    }


    private void processQueryRawEvent(Event event) {

        rowEvent.add(event);
    }


    protected Event pollEvent() {
        try {
            return deque.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    public void handlerEvents(Collection<Event> collection) {

        Iterator<Event> eventIterator = collection.iterator();

        while (eventIterator.hasNext()) {

            Event event = eventIterator.next();
            System.out.println(event);
//           if (!eventHandler(event)){
//               throw new RuntimeException("error ");
//           }


        }
    }

    @SneakyThrows
    public boolean eventHandler(Event event) {

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
    }

    @Override
    public void run() {
        work();
    }
}
