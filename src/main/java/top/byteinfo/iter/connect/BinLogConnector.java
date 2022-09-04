package top.byteinfo.iter.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;
import top.byteinfo.iter.binlog.DataEventListener;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class BinLogConnector {

    private static final Logger log = LoggerFactory.getLogger(BinLogConnector.class);

    static {
//        SLF4JBridgeHandler.install();
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    private String host;
    private Integer port;
    private String username;
    private String password;
    private BinaryLogClient client;

    private DataEventListener dataEventListener;

    private BinaryLogClient.LifecycleListener connectorListener;
    private AtomicBoolean connected;
    private int connectCount =-1;

    public BinLogConnector() {
        this("localhost", 3306, "root", "root");
    }

    public BinLogConnector(String host, Integer port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.client = new BinaryLogClient(host, port, username, password);
        this.connectorListener = new ConnectorLifeListener();
        this.connected = new AtomicBoolean(false);

        //todo
        client.registerLifecycleListener(connectorListener);

//        this.connectCount = -1;
    }

    private boolean connect() {

        log.debug("connecting ");
        Executors.newSingleThreadExecutor().submit(this::run);
        return true;
    }

    public boolean tryConnect() {
        connect();
        int count = 0;
        while (!connected.get()) {
            count += count <= 13 ? 1 : 0;
            try {
                Thread.sleep(1L << count);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (count >= 13) {
                connect();
                connectCount++;
            }
            if (connectCount>3)throw new RuntimeException("connect fail");
        }
        log.debug("connect success");
        return true;
    }

    //    @Override
    private void run() {
        try {
            client.connect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        LOGGER.info();
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

    private class ConnectorLifeListener implements BinaryLogClient.LifecycleListener {


        @Override
        public void onConnect(BinaryLogClient client) {
            boolean alive = client.isKeepAlive();
            connected.set(alive);
        }

        @Override
        public void onCommunicationFailure(BinaryLogClient client, Exception ex) {

        }

        @Override
        public void onEventDeserializationFailure(BinaryLogClient client, Exception ex) {

        }

        @Override
        public void onDisconnect(BinaryLogClient client) {

        }
    }
}
