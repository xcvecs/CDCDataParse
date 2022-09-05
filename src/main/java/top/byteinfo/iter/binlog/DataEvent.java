package top.byteinfo.iter.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import org.apache.commons.beanutils.BeanUtilsBean2;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import static top.byteinfo.iter.binlog.DataEvent.BeanListUtils.getValues;

public class DataEvent {
    private final DataEventType dataEventType;
    private final List<Event> eventList;


    public DataEvent(DataEventType dataEventType, List<Event> eventLinkedList) {
        switch (dataEventType) {
            case DDL:
            case DML:
                break;
            case BoundaryDDL:
            case BoundaryDML:
        }
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

        DDL(2, ""),
        DML(5, ""),

        BoundaryDDL(-1, "BoundaryCondition"),
        BoundaryDML(-1, "BoundaryCondition"),
        Boundary(-2,"BoundaryCondition"),

        ;

        private Integer dataCount;
        private String desp;


        DataEventType(int dataCount, String desp) {
            this.dataCount = dataCount;
            this.desp = desp;
        }

        public String getDesp() {
            return desp;
        }



        public int getDataCount() {
            return dataCount;
        }




    }

    public static class BeanListUtils {
        public static <T> List<T> getValues(List<T> sourceList) {
            List<T> targetList = new ArrayList<>(sourceList.size());
            try {
                BeanUtilsBean2.getInstance().copyProperties(targetList,sourceList);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }

            return targetList;

        }
    }
}
