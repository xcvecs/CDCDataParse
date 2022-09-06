package top.byteinfo.binlog;

import com.github.shyiko.mysql.binlog.event.Event;
import org.apache.commons.beanutils.BeanUtilsBean2;
import top.byteinfo.Jackson;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

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
        this.eventList = eventLinkedList;


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



    @Override
    public String toString() {
        return Jackson.toString(this);
    }
}
