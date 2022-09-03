package top.byteinfo.iter.binlog;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.concurrent.LinkedBlockingDeque;

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;

public class DataEventListener implements BinaryLogClient.EventListener {

    private static final Logger log= LoggerFactory.getLogger(DataEventListener.class);
    LinkedBlockingDeque<Event> blockingDeque;
    HashMap<EventType, Event> hashMap;//
    public DataEventListener(LinkedBlockingDeque<Event> blockingDeque, HashMap<EventType, Event> hashMap) {
        this.blockingDeque = blockingDeque;
        this.hashMap = hashMap;
    }

    @Override
    public void onEvent(Event event) {
//        EventType eventType = event.getHeader().getEventType();
//
//        if (eventType.equals(ROTATE) || eventType.equals(FORMAT_DESCRIPTION)) {
//            return;
//        }
//        System.out.println(event);
        EventType eventType = event.getHeader().getEventType();
        if (eventType.equals(EXT_WRITE_ROWS)) ((WriteRowsEventData) event.getData()).getRows();
        log.debug(event.getHeader().getEventType().name());
        blockingDeque.add(event);
//        hashMap.put(event.getHeader().getEventType(), event);
//        determine(hashMap);

    }

    public LinkedBlockingDeque<Event> getBlockingDeque() {
        return blockingDeque;
    }

    /*void determine(HashMap<EventType, Event> hashMap) {
        if (hashMap.containsKey(ROTATE) || hashMap.containsKey(FORMAT_DESCRIPTION)) {
            hashMap.clear();
        }
        if (hashMap.containsKey(QUERY)) {
            Event event = hashMap.get(QUERY);
            QueryEventData data = event.getData();
            if (!data.getSql().equals("BEGIN")) {
                hashMap.values().forEach(System.out::println);
                hashMap.clear();
            }
        }

        if (hashMap.size() < 5)
            return;
        if (hashMap.size() == 5) {
            Event event = hashMap.get(TABLE_MAP);
            EventData eventData = event.getData();
            if (eventData.toString().contains("source/maxwell")) {
                hashMap.clear();
                return;
            } else {
                Collection<Event> values = hashMap.values();
                values.forEach(System.out::println);
//                System.out.println("================");
                hashMap.clear();
            }
        }
        if (hashMap.size() > 5) {
            System.out.println("e@e@e");
            hashMap.clear();
        }
    }*/

}
