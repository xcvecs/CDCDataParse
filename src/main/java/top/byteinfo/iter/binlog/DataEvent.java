package top.byteinfo.iter.binlog;

import com.github.shyiko.mysql.binlog.event.Event;

import java.util.ArrayList;
import java.util.List;

import static top.byteinfo.iter.binlog.DataEvent.BeanUtils.getValues;

public class DataEvent {
    private final DataEventType dataEventType;
    private final List<Event> eventList;

    public DataEvent(DataEventType dataEventType, List<Event> eventLinkedList) {
        this.dataEventType = dataEventType;

        this.eventList = getValues(eventLinkedList);
    }

    public DataEventType getDataEventType() {
        return dataEventType;
    }

    public List<Event> getEventList() {
        return eventList;
    }

    public enum DataEventType {

        DDL(2, "ANONYMOUS_GTID--QUERY"), DML(5, "ANONYMOUS_GTID--QUERY--TABLE_MAP--EXT_DELETE_ROWS--XID");
        private Integer dataCount;
        private String desp;


        DataEventType(int dataCount, String desp) {
            this.dataCount = dataCount;
            this.desp = desp;
        }

        public static DataEventType generateType(int dataCount) {
            if (dataCount == 2) return DDL;
            if (dataCount == 5) return DML;
            throw new IllegalArgumentException(dataCount + "error");

        }

        public String getDataType() {
            return desp;
        }

        public int getDataCount() {
            return dataCount;
        }
    }

    public static class BeanUtils {

        public static  <T> List<T> getValues(List<T> list) {
            List<T> targetList = new ArrayList<>(list.size());
            targetList.addAll(list);
            return targetList;

        }
    }
}
