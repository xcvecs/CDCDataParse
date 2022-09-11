package top.byteinfo.producer;

import top.byteinfo.DycdcContext;

public abstract class AbstractProducer {
    public final DycdcContext dycdcContext;

    protected AbstractProducer(DycdcContext dycdcContext) {
        this.dycdcContext = dycdcContext;
    }

    abstract public void push(ChangedEvent cEvent) throws Exception;

}
