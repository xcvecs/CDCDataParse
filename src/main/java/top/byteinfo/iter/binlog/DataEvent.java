package top.byteinfo.iter.binlog;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.LinkedList;

public class DataEvent {


    private final DataEventType dataEventType;
    private final LinkedList<Event> ddlList;

    public DataEvent(DataEventType dataEventType, LinkedList<Event> eventLinkedList) {
        if (dataEventType.dataCount != eventLinkedList.size()) throw new RuntimeException("Event error ");
        this.dataEventType = dataEventType;
        this.ddlList = eventLinkedList;
    }

    public enum DataEventType {
        DDL(2, "DDL"),
        DML(5, "DML");
        private final Integer dataCount;
        private final String dataType;


        DataEventType(int dataCount, String dataType) {
            this.dataCount = dataCount;
            this.dataType = dataType;
        }

        public String getDataType() {
            return dataType;
        }

        public Integer getDataCount() {
            return dataCount;
        }
    }
}
