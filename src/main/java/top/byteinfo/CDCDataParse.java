package top.byteinfo;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import top.byteinfo.iter.CustomEventListener;
import top.byteinfo.iter.CustomSchemaCapture;
import top.byteinfo.iter.MaxwellBinlogReplicator;
import top.byteinfo.iter.connect.BinLogConnector;
import top.byteinfo.source.maxwell.schema.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

import static top.byteinfo.iter.CustomSchemaCapture.CaseSensitivity.CONVERT_TO_LOWER;

public class CDCDataParse implements Closeable, Runnable {

    public static final Logger logger = Logger.getLogger(CDCDataParse.class.getName());
    private final static ExecutorService executorService = Executors.newFixedThreadPool(5);

    private final static DruidDataSource ds;

    static {
        ds = new DruidDataSource();
        ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        ds.setUsername("root");
        ds.setPassword("root");
//        ds.setUrl(getConnectionURI());
        ds.setUrl("jdbc:mysql://localhost?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC");
        ds.setInitialSize(3); // 初始的连接数；
        ds.setMaxActive(10);
        ds.setMinIdle(3);
        ds.setMaxWait(3000);
    }

    private BinLogConnector binLogConnector;
    private CustomSchemaCapture schemaCapture;
    private MaxwellBinlogReplicator maxwellBinlogReplicator;
    private Schema schema;


    public CDCDataParse() {
    }

    public static void main(String[] args) {
        CDCDataParse cdcDataParse = new CDCDataParse();
        System.out.println("t");
        cdcDataParse.run();
        System.out.println("t");
    }

    public Connection getConnection() {
        try (DruidPooledConnection connection = ds.getConnection()) {
            return connection.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    public void preConnect() {
        try {
            /**
             * todo
             *
             */
            Connection connection = getConnection();
//            java.sql.DatabaseMetaData meta = connection.getMetaData();
//            int major = meta.getDatabaseMajorVersion();
            schemaCapture = new CustomSchemaCapture(connection, CONVERT_TO_LOWER);


        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void connect() {
        try {
            schema = schemaCapture.capture();
            schemaCapture.close();
        } catch (Exception e) {
            System.out.println(e);
        }

    }


    @Override
    public void run() {


        /**
         *
         */
        preConnect();
        /**
         *
         */
        binLogConnector = new BinLogConnector();
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG, EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        binLogConnector.setEventDeserializer(eventDeserializer);
        CustomEventListener customEventListener = new CustomEventListener(new LinkedBlockingDeque<>(1 << 20), new LinkedHashMap<>());
        binLogConnector.registerEventListener(customEventListener);

        /**
         * parse data
         */
        LinkedBlockingDeque<Event> blockingDeque = customEventListener.getBlockingDeque();
        maxwellBinlogReplicator = new MaxwellBinlogReplicator(blockingDeque, schema, schemaCapture);


        /**
         * 全局锁。
         */
        dbGlobalLock();
        long start = System.currentTimeMillis();
        System.out.println();
        executorService.submit(binLogConnector);// todo 添加标志位
        connect();// todo 添加标志位
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        dbGlobalUNLock();

        /**
         *
         */
        executorService.submit(maxwellBinlogReplicator);
    }

    public void dbGlobalLock() {

    }

    public void dbGlobalUNLock() {

    }

    @Override
    public void close() throws IOException {

    }
}
