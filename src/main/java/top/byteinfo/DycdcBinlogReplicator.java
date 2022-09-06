package top.byteinfo;

import com.github.shyiko.mysql.binlog.event.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.byteinfo.binlog.AtomEvent;
import top.byteinfo.binlog.DataEvent;
import top.byteinfo.binlog.NativeDataEvent;
import top.byteinfo.connector.DycdcConnector;
import top.byteinfo.producer.AbstractProducer;
import top.byteinfo.producer.ChangedEvent;
import top.byteinfo.schema.*;
import top.byteinfo.schema.columndef.ColumnDef;
import top.byteinfo.schema.columndef.ColumnDefCastException;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static top.byteinfo.schema.GlobalConstant.BEGIN;

public class DycdcBinlogReplicator implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(DycdcBinlogReplicator.class);

    private final LinkedBlockingDeque<Event> deque;

    private final DycdcConnector dycdcConnector;
    private final Schema schema;
    private final SchemaCapture schemaCapture;
    private final DycdcContext dycdcContext;

    private final TableCache tableCache = new TableCache();
    private final NativeDataEvent nativeDataEvent = new NativeDataEvent();


    public DycdcBinlogReplicator(
            DycdcConnector dycdcConnector,
            LinkedBlockingDeque<Event> deque,
            Schema schema,
            SchemaCapture schemaCapture,
            DycdcContext dycdcContext
    ) {
        this.dycdcConnector = dycdcConnector;
        this.deque = deque;
        this.schema = schema;
        this.schemaCapture = schemaCapture;
        this.dycdcContext = dycdcContext;
    }


    public DycdcConnector getDycdcConnector() {
        return dycdcConnector;
    }

    public Schema getSchema() {
        return schema;
    }

    public SchemaCapture getSchemaCapture() {
        return schemaCapture;
    }


    public DycdcContext getDycdcContext() {
        return dycdcContext;
    }

    protected Event pollEvent() {
        try {
            return deque.poll(100, TimeUnit.MILLISECONDS);
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
            int sleepCount = 0;
            sleepCount += timeCount++ < 3 ? 3 : 0;
            try {
                Thread.sleep(1L << sleepCount);
            } catch (InterruptedException e) {
                log.info("sleep count:" + timeCount);
            }
            event = pollEvent();
        }
        return event;
    }

    private Event ensureEvent(AtomEvent atomEvent) {
        // TODO
        Event event = ensureEvent();
        atomEvent.add(event);
        return event;
    }

    public void maxwellParseEvent() {
        while (true) {
            DataEvent dataEvent = getDataEvent(new AtomEvent());
            log.debug("dataEvent:" + dataEvent);
            Table table = tableCache.getTable();


            DataEvent.DataEventType dataEventType = dataEvent.getDataEventType();
            switch (dataEventType) {
                case DML:
                    List<Event> eventList = dataEvent.getEventList();
                    Event event = eventList.get(3);
                    String changedEventData = parseBin(event);

                    Event xidEvent = eventList.get(4);
                    XidEventData xidEventData = xidEvent.getData();



                    long ts = xidEvent.getHeader().getTimestamp();
                    long xid = xidEventData.getXid();


                    ChangedEvent changedEvent =  new ChangedEvent(table.database,table.name,ts,xid,"true",changedEventData);
                    try {
                        dycdcContext.getProducer().push(changedEvent);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    break;
                default:
                    log.warn("");
            }


        }

    }

  public   String parseBin(Event event)  {

        try {


            Table table = tableCache.getTable();
            List<ColumnDef> defList = table.getColumns().getList();
            StringBuilder changedEventString = new StringBuilder();

            switch (event.getHeader().getEventType()) {
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                    WriteRowsEventData eventData = event.getData();
                    List<Serializable[]> rows = eventData.getRows();
                    List<String> rowList = new ArrayList<>();
                    for (Serializable[] row : rows) {
                        for (int i = 0; i < row.length; i++) {
                            if (row[i] != null) {
                                String rowString = defList.get(i).toSQL(row[i]);
                                rowList.add(rowString);
                            }
                        }

                    }
                    changedEventString.append(new ArrayList<>()).append(rowList);
                    break;
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                    DeleteRowsEventData deleteRowsEventData = event.getData();
                    List<Serializable[]> deleteRowsEventDataRows = deleteRowsEventData.getRows();

                    List<String> deleteRowList = new ArrayList<>();
                    for (Serializable[] deleteRow : deleteRowsEventDataRows) {
                        for (int i = 0; i < deleteRow.length; i++) {
                            if (deleteRow[i] == null) continue;
                            String rowString = defList.get(i).toSQL(deleteRow[i]);
                            deleteRowList.add(rowString);
                        }
                    }
                    changedEventString.append(deleteRowList).append(new ArrayList<>());//todo
                    break;
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                    UpdateRowsEventData updateRowsEventData = event.getData();
                    List<Map.Entry<Serializable[], Serializable[]>> updateRowsEventDataRows = updateRowsEventData.getRows();

                    for (Map.Entry<Serializable[], Serializable[]> updateRowsEventDataRow : updateRowsEventDataRows) {
                        Serializable[] key = updateRowsEventDataRow.getKey();
                        Serializable[] value = updateRowsEventDataRow.getValue();
                        List<String> kList = new ArrayList<>();
                        List<String> vList = new ArrayList<>();
                        for (int i = 0; i < key.length; i++) {
                            if (key[i] == null) continue;
                            ColumnDef columnDef = defList.get(i);

                            String keyString = columnDef.toSQL(key[i]);
                            String valueString = columnDef.toSQL(value[i]);


                            kList.add(keyString);
                            vList.add(valueString);
                        }
                        changedEventString.append(kList).append(vList);//todo
                    }
                    break;
                default:
                    throw new RuntimeException();
            }


            return changedEventString.toString();
        }
        catch (Exception e){
            log.error("error"+e);
        }
        return null;
    }

    public DataEvent getDataEvent(AtomEvent atomEvent) {
        Event event;


        while (true) {
//            if (null != nativeEventBuffer && !nativeEventBuffer.isEmpty()) {
//                Iterator<NativeDataEvent> iterator = nativeEventBuffer.iterator();
//                if (iterator.hasNext()) {
//                    NativeDataEvent nativeEvent = iterator.next();
//                    log.info("NativeDataEvent:" + nativeEvent);
//                    return nativeEvent.removeFirst();
//                }
//                nativeEventBuffer = null;
//            }
            if (nativeDataEvent.generated()) {
                DataEvent dataEvent = nativeDataEvent.removeFirst();
                log.debug("NativeDataEvent:" + nativeDataEvent);
                nativeDataEvent.clear();
                return dataEvent;
            }
            event = ensureEvent(atomEvent);


            switch (event.getHeader().getEventType()) {
                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                    log.warn("Started replication stream inside a transaction.  This shouldn't normally happen.");
                    log.warn("Assuming new transaction at unexpected event:" + event);

                    deque.offerFirst(event);
//                    nativeEventBuffer = getTransactionDataEvent(event);
                    nativeDataEvent.generate(getTransactionDataEvent(atomEvent));
//                    nativeDataEvent = getTransactionDataEvent(event);
                    break;
                case TABLE_MAP:
                    log.warn("");
                    break;
                case QUERY:
                    QueryEventData qe = event.getData();
                    String sql = qe.getSql();
                    if (sql.equals(BEGIN.name())) {
                        try {
//                            nativeEventBuffer = getTransactionDataEvent(event);
                            nativeDataEvent.generate(getTransactionDataEvent(atomEvent));
//                            nativeDataEvent = getTransactionDataEvent(event);
                        } catch (Exception e) {

                            break;
                        }
                        nativeDataEvent.setServerId(event.getHeader().getServerId());
                        nativeDataEvent.setThreadId(qe.getThreadId());
                        nativeDataEvent.setSchemaId(Long.valueOf(schema.getSchemaId()));

                    } else {
                        processQueryEvent(event);
                    }
                    break;
                case ROTATE:
                case FORMAT_DESCRIPTION:

                    log.info("format");
//                    tableCache.clear();
                    break;
                default:
                    break;
            }
        }
    }

    private AtomEvent getTransactionDataEvent(AtomEvent atomEvent) {
        Event event;

        String currentQuery = null;

        while (true) {
            event = ensureEvent(atomEvent);

            EventType eventType = event.getHeader().getEventType();
            if (eventType == EventType.XID) {
                return atomEvent;
            }
            switch (eventType) {
                case WRITE_ROWS:
                case UPDATE_ROWS:
                case DELETE_ROWS:
                case EXT_WRITE_ROWS:
                case EXT_UPDATE_ROWS:
                case EXT_DELETE_ROWS:
//                    Table table = tableCache.getTable();
//                    currentQuery = null;
                    break;
                case TABLE_MAP:
                    TableMapEventData data = event.getData();
                    List<Integer> columnCharsets = data.getEventMetadata().getColumnCharsets();
                    boolean processEvent = tableCache.processEvent(getSchema(), data);//todo BoundaryCondition
                    if (processEvent) log.debug("");
                    else log.error("bad");
                    break;
                case ROWS_QUERY:
//                    RowsQueryEventData rqed = event.getData();
//                    currentQuery = rqed.getQuery();
                    break;
                case QUERY:
                    QueryEventData qe = event.getData();
                    String sql = qe.getSql();
//                    String upperCaseSql = sql.toUpperCase();
                    Pattern pattern = Pattern.compile("^CREATE\\s+TABLE", Pattern.CASE_INSENSITIVE);
                    if (pattern.matcher(sql).find()) processQueryEvent(event);
                    log.warn("ddl in transaction");//todo
                    break;
            }
        }

    }

    private void processQueryEvent(Event event) {

    }


    @Override
    public void run() {
        maxwellParseEvent();
    }
}
