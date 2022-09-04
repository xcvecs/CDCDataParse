package top.byteinfo.iter.producer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.XidEventData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.byteinfo.iter.DataParseContext;

public class StdoutProducer extends AbstractProducer {

    private static final Logger log = LoggerFactory.getLogger(StdoutProducer.class);

    public StdoutProducer(DataParseContext dataParseContext) {
        super(dataParseContext);
    }

    @Override
    public void push(ChangedEvent cEvent) throws Exception {
        String event = JSON.toJSONString(cEvent, SerializerFeature.SortField);
        log.debug(event);
        System.out.println(event);
    }


}
