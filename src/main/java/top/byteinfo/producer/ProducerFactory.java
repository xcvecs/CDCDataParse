package top.byteinfo.producer;


import top.byteinfo.DycdcContext;

public interface ProducerFactory {

    AbstractProducer createProducer(DycdcContext context);
}
