package top.byteinfo.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.byteinfo.DycdcContext;

public class StdoutProducer extends AbstractProducer {

    private static final Logger log = LoggerFactory.getLogger(StdoutProducer.class);

    public StdoutProducer(DycdcContext dycdcContext) {
        super(dycdcContext);
    }

    @Override
    public void push(ChangedEvent cEvent) throws Exception {

        ObjectMapper objectMapper = new ObjectMapper();
        String event = objectMapper.writeValueAsString(cEvent);


        log.info(event);
    }


}
