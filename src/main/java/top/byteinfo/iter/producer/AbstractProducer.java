package top.byteinfo.iter.producer;

import com.github.shyiko.mysql.binlog.event.Event;
import top.byteinfo.iter.DataParseContext;

public abstract class AbstractProducer {
    public final DataParseContext dataParseContext;

    protected AbstractProducer(DataParseContext dataParseContext) {
        this.dataParseContext = dataParseContext;
    }

    abstract public void push(ChangedEvent cEvent) throws Exception;

}
