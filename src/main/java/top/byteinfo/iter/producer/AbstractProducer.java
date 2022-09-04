package top.byteinfo.iter.producer;

import top.byteinfo.iter.DataParseContext;

public abstract class AbstractProducer {
    public final DataParseContext dataParseContext;

    protected AbstractProducer(DataParseContext dataParseContext) {
        this.dataParseContext = dataParseContext;
    }

    abstract public void push(ChangedEvent cEvent) throws Exception;
}
