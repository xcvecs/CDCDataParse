package top.byteinfo.iter.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import top.byteinfo.iter.binlog.DataEventListener;

import java.io.IOException;

public class BinLogConnector implements Runnable {

    private String host;
    private Integer port;
    private String username;
    private String password;
    private BinaryLogClient client;

    private DataEventListener dataEventListener;

    public BinLogConnector() {
        this("localhost", 3306, "root", "root");
    }

    public BinLogConnector(String host, Integer port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.client = new BinaryLogClient(host, port, username, password);
    }

    @Override
    public void run() {


        try {
            client.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean setEventDeserializer(EventDeserializer eventDeserializer) {
        client.setEventDeserializer(eventDeserializer);
        return true;
    }

    public boolean registerEventListener(DataEventListener eventListener) {
        client.registerEventListener(eventListener);
        dataEventListener = eventListener;
        return true;
    }

    public DataEventListener getEventListener() {
        return dataEventListener;
    }


}
