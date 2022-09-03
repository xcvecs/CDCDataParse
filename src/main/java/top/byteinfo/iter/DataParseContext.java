package top.byteinfo.iter;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.byteinfo.iter.binlog.DataEventListener;
import top.byteinfo.iter.connect.BinLogConnector;
import top.byteinfo.iter.schema.Schema;
import top.byteinfo.iter.schema.SchemaCapture;
import top.byteinfo.iter.schema.ServerCaseSensitivity;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

public class DataParseContext {

     private static final Logger log = LoggerFactory.getLogger(DataParseContext.class);
    private final DataParseConfig dataParseConfig;
    private DataSource dataSource;
    private BinLogConnector binLogConnector;
    private SchemaCapture schemaCapture;
    private MaxwellBinlogReplicator maxwellBinlogReplicator;
    private Schema schema;


    public DataParseContext(DataParseConfig dataParseConfig) {
        this.dataParseConfig = dataParseConfig;
        log.debug("DataParseContext 初始化 start");
        setup();
        log.debug("DataParseContext 初始化 end");


        log.debug(" parse data 预处理 start ");
        parsePre();
        log.debug(" parse data 预处理  end");

    }


    public DataParseConfig getDataParseConfig() {
        return dataParseConfig;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public BinLogConnector getBinLogConnector() {
        return binLogConnector;
    }

    public MaxwellBinlogReplicator getMaxwellBinlogReplicator() {
        return maxwellBinlogReplicator;
    }

    public Schema getSchema() {
        return schema;
    }

    public SchemaCapture getSchemaCapture() {
        return schemaCapture;
    }

    public void setup() {

        setupDataSource();
        setupSchemaCapture();
        setupBinlogConnect();
        setupBinlogReplicator();
    }

    public void parsePre() {
        log.debug("获取数据库 数据模型 start");
        long l1 = dbGlobalLock();
        schema = dataModelCapture();
        Executors.newSingleThreadExecutor().submit(binLogConnector);
        long l2 = dbGlobalUNLock();
        log.debug(" time:" + (l2 - l1));
        log.debug("获取数据库 数据模型 end");

    }


    public void setupDataSource() {
        Properties properties = dataParseConfig.getProperties();

        final HikariConfig config = new HikariConfig();
        config.setPoolName("dataSourcePool");
        config.setDriverClassName(properties.getProperty("dataSource.setDriverClassName"));
        config.setUsername(properties.getProperty("dataSource.setUsername"));
        config.setPassword(properties.getProperty("dataSource.setPassword"));
        config.setJdbcUrl(properties.getProperty("dataSource.setJdbcUrl"));

//        properties.setProperty("dataSource.setInitialSize", "5");
//        properties.setProperty("dataSource.setMaxActive", "10");
//        properties.setProperty("dataSource.setMinIdle", "3");
//        properties.setProperty("dataSource.setMaxWait", "3000");

//        config.setMaximumPoolSize(Integer.parseInt(properties.getProperty("dataSource.setInitialSize")));
//        config.setMinimumIdle(Integer.parseInt(properties.getProperty("dataSource.setMinIdle")));
//        config.setMaxLifetime(Long.parseLong(properties.getProperty("dataSource.setMaxWait")));

//       DataSource dataSource = new DruidDataSource();
//        dataSource.setDriverClassName(properties.getProperty("dataSource.setDriverClassName"));
//        dataSource.setUsername(properties.getProperty("dataSource.setUsername"));
//        dataSource.setPassword(properties.getProperty("dataSource.setPassword"));
//        dataSource.setUrl(properties.getProperty("dataSource.setUrl"));

//        dataSource.setInitialSize(Integer.parseInt(properties.getProperty("dataSource.setInitialSize")));
//        dataSource.setMaxActive(Integer.parseInt(properties.getProperty("dataSource.setMaxActive")));
//        dataSource.setMinIdle(Integer.parseInt(properties.getProperty("dataSource.setMinIdle")));
//        dataSource.setMaxWait(Long.parseLong(properties.getProperty("dataSource.setMaxWait")));

        HikariDataSource hDataSource = new HikariDataSource(config);
            this.dataSource = hDataSource;



    }

    public void setupSchemaCapture() {
        try {
            Connection connection = dataSource.getConnection();
            ServerCaseSensitivity caseSensitivity = DataBaseServerStatus.MaxwellMysqlStatus.captureCaseSensitivity(connection);
            SchemaCapture capture = new SchemaCapture(connection, caseSensitivity);
            this.schemaCapture = capture;

            Schema schema = capture.capture();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setupBinlogConnect() {
        binLogConnector = new BinLogConnector();
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        binLogConnector.setEventDeserializer(eventDeserializer);
        String capacity = dataParseConfig.getProperties().getProperty("binLogConnector.registerEventListener");
        DataEventListener dataEventListener = new DataEventListener(new LinkedBlockingDeque<>(Integer.parseInt(capacity)), new LinkedHashMap<>());
        binLogConnector.registerEventListener(dataEventListener);
    }


    public void setupBinlogReplicator() {
        LinkedBlockingDeque<Event> blockingDeque = binLogConnector.getEventListener().getBlockingDeque();
        maxwellBinlogReplicator = new MaxwellBinlogReplicator(blockingDeque, schema, schemaCapture);
    }

    public Schema dataModelCapture() {
        log.debug("");
        try {
            this.schema = schemaCapture.capture();
            return this.schema;
        } catch (Exception e) {
            System.out.println(e);
        }
        throw new NullPointerException("");
    }

    public long dbGlobalLock() {
        long start = System.currentTimeMillis();
        return start;
    }

    public long dbGlobalUNLock() {
        long end = System.currentTimeMillis();
        return end;
    }
}
