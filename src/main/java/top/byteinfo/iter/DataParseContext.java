package top.byteinfo.iter;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import top.byteinfo.iter.connect.BinLogConnector;
import top.byteinfo.source.maxwell.schema.CustomSchema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

import static top.byteinfo.iter.CustomSchemaCapture.CaseSensitivity.CONVERT_TO_LOWER;

public class DataParseContext {
    private final DataParseConfig dataParseConfig;
    private DruidDataSource druidDataSource;
    private BinLogConnector binLogConnector;
    private CustomSchemaCapture schemaCapture;
    private MaxwellBinlogReplicator maxwellBinlogReplicator;
    private CustomSchema customSchema;


    public DataParseContext(DataParseConfig dataParseConfig) {
        this.dataParseConfig = dataParseConfig;
        setup();



        parsePre();
    }


    public DataParseConfig getDataParseConfig() {
        return dataParseConfig;
    }

    public DruidDataSource getDruidDataSource() {
        return druidDataSource;
    }

    public BinLogConnector getBinLogConnector() {
        return binLogConnector;
    }

    public MaxwellBinlogReplicator getMaxwellBinlogReplicator() {
        return maxwellBinlogReplicator;
    }

    public CustomSchema getSchema() {
        return customSchema;
    }

    public CustomSchemaCapture getSchemaCapture() {
        return schemaCapture;
    }

    public void setup() {
        setupDataSource();
        setupSchemaCapture();
        setupBinlogConnect();
        setupBinlogReplicator();

    }

    public void parsePre() {
        long l1 = dbGlobalLock();
        customSchema = dataModelCapture();
        binLogConnector.run();
        long l2 = dbGlobalUNLock();
        System.out.println(l2 - l1);
    }


    public void setupDataSource() {
        Properties properties = dataParseConfig.getProperties();
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.getProperty("dataSource.setDriverClassName"));
        dataSource.setUsername(properties.getProperty("dataSource.setUsername"));
        dataSource.setPassword(properties.getProperty("dataSource.setPassword"));
        dataSource.setUrl(properties.getProperty("dataSource.setUrl"));
        dataSource.setInitialSize(Integer.parseInt(properties.getProperty("dataSource.setInitialSize")));
        dataSource.setMaxActive(Integer.parseInt(properties.getProperty("dataSource.setMaxActive")));
        dataSource.setMinIdle(Integer.parseInt(properties.getProperty("dataSource.setMinIdle")));
        dataSource.setMaxWait(Long.parseLong(properties.getProperty("dataSource.setMaxWait")));
        this.druidDataSource = dataSource;
    }

    public void setupSchemaCapture() {
        try {
            Connection connection = druidDataSource.getConnection().getConnection();
            schemaCapture = new CustomSchemaCapture(connection, CONVERT_TO_LOWER);
            new SchemaCapture(connection,MaxwellMysqlStatus.captureCaseSensitivity(connection));
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
        CustomEventListener customEventListener = new CustomEventListener(new LinkedBlockingDeque<>(Integer.parseInt(capacity)), new LinkedHashMap<>());
        binLogConnector.registerEventListener(customEventListener);
    }


    public void setupBinlogReplicator() {
        LinkedBlockingDeque<Event> blockingDeque = binLogConnector.getEventListener().getBlockingDeque();
        maxwellBinlogReplicator = new MaxwellBinlogReplicator(blockingDeque, customSchema, schemaCapture);
    }

    public CustomSchema dataModelCapture() {
        try {
            CustomSchema customSchema = schemaCapture.capture();
            List<String> databaseNames = customSchema.getDatabaseNames();
            String schemaCharset = customSchema.getCharset();

            return customSchema;
        } catch (Exception e) {
            System.out.println(e);
        }
        throw new NullPointerException("");
    }

    public long dbGlobalLock() {
        long start = System.currentTimeMillis();
        System.out.println(start);
        return start;
    }

    public long dbGlobalUNLock() {
        long end = System.currentTimeMillis();
        System.out.println(end);
        return end;
    }
}
