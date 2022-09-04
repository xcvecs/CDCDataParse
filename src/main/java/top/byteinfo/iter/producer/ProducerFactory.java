package top.byteinfo.iter.producer;


import top.byteinfo.iter.DataParseContext;

public interface ProducerFactory {

    AbstractProducer createProducer(DataParseContext context);
}
