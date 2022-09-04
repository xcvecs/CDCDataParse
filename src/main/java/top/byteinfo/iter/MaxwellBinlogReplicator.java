package top.byteinfo.iter;

import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.byteinfo.iter.binlog.DataEvent;
import top.byteinfo.iter.producer.AbstractProducer;
import top.byteinfo.iter.producer.ChangedEvent;
import top.byteinfo.iter.schema.DataBase;
import top.byteinfo.iter.schema.Schema;
import top.byteinfo.iter.schema.SchemaCapture;
import top.byteinfo.iter.schema.Table;
import top.byteinfo.iter.schema.TableColumnList;
import top.byteinfo.iter.schema.columndef.ColumnDef;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static com.github.shyiko.mysql.binlog.event.EventType.DELETE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.FORMAT_DESCRIPTION;
import static com.github.shyiko.mysql.binlog.event.EventType.QUERY;
import static com.github.shyiko.mysql.binlog.event.EventType.ROTATE;
import static com.github.shyiko.mysql.binlog.event.EventType.ROWS_QUERY;
import static com.github.shyiko.mysql.binlog.event.EventType.UPDATE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.WRITE_ROWS;
import static top.byteinfo.iter.binlog.DataEvent.DataEventType.DDL;
import static top.byteinfo.iter.binlog.DataEvent.DataEventType.DML;

public class MaxwellBinlogReplicator implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(MaxwellBinlogReplicator.class);
    private final LinkedBlockingDeque<Event> deque;
    private final LinkedBlockingDeque<DataEvent> dataEvents;
    private final LinkedList<Event> rowEventList;
    private Schema schema;
    private SchemaCapture schemaCapture;
    private DataParseContext dataParseContext;

    private LinkedHashMap<String, Table> tableCache;

    public MaxwellBinlogReplicator(
            LinkedBlockingDeque<Event> deque,
            Schema schema,
            SchemaCapture schemaCapture,
            LinkedList<Event> rowEvent,
            LinkedHashMap<String, Table> tableCache,
            DataParseContext dataParseContext
    ) {

        this.deque = deque;
        this.schema = schema;
        this.schemaCapture = schemaCapture;
        this.rowEventList = rowEvent;
        this.tableCache = tableCache;
        this.dataEvents = new LinkedBlockingDeque<>();
        this.dataParseContext = dataParseContext;
    }

    public MaxwellBinlogReplicator(LinkedBlockingDeque<Event> linkedBlockingDeque, Schema schema, SchemaCapture schemaCapture, DataParseContext dataParseContext) {
        this(linkedBlockingDeque, schema, schemaCapture, new LinkedList<>(), new LinkedHashMap<>(), dataParseContext);
    }


    protected Event pollEvent() {
        try {
            return deque.poll(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("...");
            return null;
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

    public DataEvent getDataEvent(){
        return null;
    }

    public void routeIter() {

    }
    public void maxwellRoute() {

        boolean eventFormat = false;
        boolean binlogValid = false;
        boolean parseFlag = false;
        List<Event> atomEvent = new ArrayList<>();
        while (true) {
            if (!eventFormat) {
                Event event0 = ensureEvent();
                Event event1 = pollEvent();
                if (event0.getHeader().getEventType().equals(ROTATE)) binlogValid = true;
                if (event1.getHeader().getEventType().equals(FORMAT_DESCRIPTION)) parseFlag = true;
                if (binlogValid && parseFlag) eventFormat = true;
                //
            }
//            AtomEvent atomEvent= new AtomEvent();
            Event event = ensureEvent();
            switch (event.getHeader().getEventType()) {
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                    //todo
                    log.error(event.toString());
                    break;
                case TABLE_MAP:
                    //todo
                    log.error(event.toString() + "");
                    break;
                case QUERY:// end
                    DataEvent dataEvent;
                    QueryEventData qe = event.getData();
                    String sql = qe.getSql();
                    if ("BEGIN".equals(sql)) {
                        try {
                            handlerDMLTransactionRows(event, atomEvent);
                            dataEvent = new DataEvent(DML, atomEvent);
                        } catch (RuntimeException e) {
                            break;
                        }
                    } else {
                        handlerDDLEvent(event, atomEvent);
                        dataEvent = new DataEvent(DDL, atomEvent);

                    }
                    dataEvents.add(dataEvent);
//                    dataEventParse(dataEvent);
                    atomEvent.clear();
                    break;
                case ROTATE:
                    log.debug(event.toString());
                    break;
                case FORMAT_DESCRIPTION:
                    log.debug("binlogfile start");
                    break;
                case ANONYMOUS_GTID://start
                    atomEvent.add(event);
                default:
                    break;
            }
        }
    }

    private void handlerDMLTransactionRows(Event event, List<Event> atomEvent) {
        atomEvent.add(event);

        while (true) {
            event = ensureEvent();


            EventType eventType = event.getHeader().getEventType();


            if (eventType == EventType.XID) {

                atomEvent.add(event);
                return;
            }

            //todo  BoundaryCondition
            switch (eventType) {
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                    log.debug("handlerDMLTransactionRows -- " + "data changed:" + WRITE_ROWS);
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                    log.debug("handlerDMLTransactionRows -- " + "data changed:" + DELETE_ROWS);
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                    dataEventParse(event);
                    atomEvent.add(event);
                    log.debug("handlerDMLTransactionRows -- " + "data changed:" + UPDATE_ROWS);
                    // add event
                    break;
                case TABLE_MAP://start
                    TableMapEventData eventData = event.getData();
                    String database = eventData.getDatabase();
                    String tableName = eventData.getTable();
                    long tableId = eventData.getTableId();
                    byte[] columnTypes = eventData.getColumnTypes();
                    Map<String, DataBase> dataBaseMap = schema.getDbMap();
                    DataBase dataBase = dataBaseMap.get(database);
                    List<Table> tableList = dataBase.getTableList();
                    for (Table table : tableList) {
                        if (table.name.equals(tableName))
                            tableCache.put(String.valueOf(tableId), table);
                    }
                    atomEvent.add(event);
                    break;
                case ROWS_QUERY:
                    log.error(ROWS_QUERY.name());
                    break;
                case QUERY:
                    log.error(QUERY.name());
                    break;
            }
        }
    }

    private void handlerDDLEvent(Event event, List<Event> atomEvent) {
        // update schema
        atomEvent.add(event);
        handlerSchemaChanged(event);
    }

    private void handlerSchemaChanged(Event event) {

    }

    private void handlerTransactionRows(Event event) {

    }


    public void parseEventData() {

        log.debug("异步预处理数据 0");
        Executors.newSingleThreadExecutor().submit(this::maxwellRoute);
        log.debug("异步预处理数据 1");

        int count = 0;
        while (true) {
            int size = deque.size();
            int sizes = dataEvents.size();
            log.debug(String.valueOf(sizes));
            count += count >= 10 ? 0 : 2;
            try {
                Thread.sleep(1L << count);
            } catch (InterruptedException e) {
                log.warn(e.getMessage());
            }
        }
    }

    public boolean dataEventParse(DataEvent dataEvent) {
        List<Event> eventList = dataEvent.getEventList();
        EventType eventHeader = eventList.stream().findFirst().get().getHeader().getEventType();
        switch (dataEvent.getDataEventType()){
            case DML:
                System.out.println("//todo0");//todo
                break;
            case DDL:
                System.out.println("//todo1");//todo
                break;
            default:
                log.error("error");
        }


        AbstractProducer producer = dataParseContext.getProducer();

//        DataEvent dataEvent = dataEvents.p;

        return true;
    }

    public boolean dataEventParse(Event event) {

        Table table = tableCache.values().stream().findAny().get();

        TableColumnList columnDefs = table.getColumns();
        List<ColumnDef> columnDefList = columnDefs.getList();

        String data = null;


        try {
            switch (event.getHeader().getEventType()) {
                case EXT_WRITE_ROWS:
                case WRITE_ROWS:
                    LinkedHashMap<String,String> wrowChanged = new LinkedHashMap<>();
                    WriteRowsEventData writeRows = event.getData();
                    List<Serializable[]> rows = writeRows.getRows();
                    for (int i = 0; i < rows.size(); i++) {
                        for (int j = 0; j < columnDefList.size(); j++) {
                            String s = columnDefList.get(j).toSQL(rows.get(i)[j]);
                            wrowChanged.put(columnDefList.get(j).getName(),s);
                            System.out.print("");
                        }
                    }
                    data = wrowChanged.toString();
                    System.out.println("write");//todo
                    break;
                case EXT_DELETE_ROWS:
                case DELETE_ROWS:
                    LinkedHashMap<String,String> drowChanged = new LinkedHashMap<>();
                    DeleteRowsEventData driteRows = event.getData();
                    List<Serializable[]> drows = driteRows.getRows();
                    for (int i = 0; i < drows.size(); i++) {
                        for (int j = 0; j < columnDefList.size(); j++) {
                            String s = columnDefList.get(j).toSQL(drows.get(i)[j]);
                            drowChanged.put(columnDefList.get(j).getName(),s);
                            System.out.print("");
                        }
                    }
                    data = drowChanged.toString();
                    System.out.println("delete");//todo
                    break;
                case EXT_UPDATE_ROWS:
                case UPDATE_ROWS:
                    LinkedHashMap<String,String> urowChanged0 = new LinkedHashMap<>();
                    LinkedHashMap<String,String> urowChanged1 = new LinkedHashMap<>();
                    SimpleEntry<LinkedHashMap<String, String>, LinkedHashMap<String, String>> rowEntry = new SimpleEntry<>(urowChanged0,urowChanged1);
                    UpdateRowsEventData updateRowsEventData = event.getData();
                    List<Entry<Serializable[], Serializable[]>> entryList = updateRowsEventData.getRows();
                    for (int i = 0; i < entryList.size(); i++) {
                        Entry<Serializable[], Serializable[]> entry = entryList.get(i);
                        for (int j = 0; j < columnDefList.size(); j++) {
                            String s = columnDefList.get(j).toSQL(entryList.get(i).getValue()[j]);
                            urowChanged0.put(columnDefList.get(j).getName(),s);
                            String s1 = columnDefList.get(j).toSQL(entryList.get(i).getKey()[j]);
                            urowChanged1.put(columnDefList.get(j).getName(),s1);
                        }
                    }
                    data = urowChanged0 + " => " + urowChanged1;
                    break;
            }
        } catch (Exception e) {

            throw new RuntimeException();
        }

        AbstractProducer producer = dataParseContext.getProducer();
//        DataEvent dataEvent = dataEvents.p;

        ChangedEvent changedEvent = new ChangedEvent(table.database,table.name,1l,null,"false",data);
        try {
            producer.push(changedEvent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
