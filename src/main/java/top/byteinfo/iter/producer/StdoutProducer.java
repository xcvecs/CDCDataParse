package top.byteinfo.iter.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
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
//        String event = JSON.toJSONString(cEvent, SerializerFeature.SortField);
        ObjectMapper objectMapper = new ObjectMapper();
        String value = objectMapper.writeValueAsString(cEvent);
        log.debug(value);
    }


}
