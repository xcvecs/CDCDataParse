package top.byteinfo.iter.binlog;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.LinkedList;

public class DataEvent {


    private final DataEventType dataEventType;
    private final LinkedList<Event> ddlList;

    public DataEvent(DataEventType dataEventType, LinkedList<Event> eventLinkedList) {
        this.dataEventType = dataEventType;
        this.ddlList = eventLinkedList;
    }

    public enum DataEventType {

        DDL(2, "DDL"),
        DML(5, "DML");
        private Integer dataCount;
        private String dataType;


        DataEventType(int dataCount, String dataType) {
            this.dataCount = dataCount;
            this.dataType = dataType;
        }

        public static DataEventType generateType(int dataCount) {
            if (dataCount == 2) return DDL;
            if (dataCount == 3) return DML;
            throw new IllegalArgumentException(dataCount + "error");

        }

        public String getDataType() {
            return dataType;
        }

        public int getDataCount() {
            return dataCount;
        }
    }
}
