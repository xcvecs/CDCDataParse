package com.zendesk.maxwell.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;

import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.FORMAT_DESCRIPTION;
import static com.github.shyiko.mysql.binlog.event.EventType.ROTATE;
import static com.github.shyiko.mysql.binlog.event.EventType.TABLE_MAP;
import static com.github.shyiko.mysql.binlog.network.protocol.command.CommandType.QUERY;

public class DataEventListener implements BinaryLogClient.EventListener {

	private static final Logger log = LoggerFactory.getLogger(DataEventListener.class);
	private final LinkedBlockingDeque<Event> blockingDeque;

	public DataEventListener(LinkedBlockingDeque<Event> blockingDeque) {
		this.blockingDeque = blockingDeque;
	}

	@Override
	public void onEvent(Event event) {
//        EventType eventType = event.getHeader().getEventType();
//
//        if (eventType.equals(ROTATE) || eventType.equals(FORMAT_DESCRIPTION)) {
//            return;
//        }
		EventType eventType = event.getHeader().getEventType();
		if (eventType.equals(EXT_WRITE_ROWS)) ((WriteRowsEventData) event.getData()).getRows();
		log.debug("eventType:" + event.getHeader().getEventType().name());
		blockingDeque.add(event);
//        preDebug(event);

	}

	public LinkedBlockingDeque<Event> getBlockingDeque() {
		return blockingDeque;
	}


	void preDebug(Event event) {

		switch (event.getHeader().getEventType()) {
			case WRITE_ROWS:
			case EXT_WRITE_ROWS:
				WriteRowsEventData wtData = event.getData();
				List<Serializable[]> wtDataRows = wtData.getRows();
				break;
			case UPDATE_ROWS:
			case EXT_UPDATE_ROWS:
				UpdateRowsEventData uData = event.getData();
				List<Entry<Serializable[], Serializable[]>> uDataRows = uData.getRows();
				break;
			case DELETE_ROWS:
			case EXT_DELETE_ROWS:
				DeleteRowsEventData dData = event.getData();
				List<Serializable[]> dDataRows = dData.getRows();
				//todo
				break;
			case TABLE_MAP:
				//todo
				log.error(event.toString() + "");
				break;
			case QUERY:// end

				break;
			case ROTATE:
				log.debug(event.toString());
				break;
			case FORMAT_DESCRIPTION:
				log.debug("binlogfile start");
				break;
			case ANONYMOUS_GTID://start
			default:
				break;
		}
	}

	void determine(HashMap<EventType, Event> hashMap) {
		if (hashMap.containsKey(ROTATE) || hashMap.containsKey(FORMAT_DESCRIPTION)) {
			hashMap.clear();
		}
		if (hashMap.containsKey(QUERY)) {
			Event event = hashMap.get(QUERY);
			QueryEventData data = event.getData();
			if (!data.getSql().equals("BEGIN")) {
				hashMap.values().forEach(value -> log.info(String.valueOf(value)));
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
				values.forEach(value -> log.info("" + value));
				hashMap.clear();
			}
		}
		if (hashMap.size() > 5) {
			log.error("e");
			hashMap.clear();
		}
	}

}
