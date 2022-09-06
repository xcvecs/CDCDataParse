package top.byteinfo.producer;

public class DataEventConfig {
    private enum DataEventOutputType{
        RabbitMq,Kafka,Redis,Stdout,log
    }


}
