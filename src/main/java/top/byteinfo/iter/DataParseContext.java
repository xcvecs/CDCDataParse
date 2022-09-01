package top.byteinfo.iter;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import top.byteinfo.iter.connect.BinLogConnector;
import top.byteinfo.source.maxwell.schema.Schema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

import static top.byteinfo.iter.CustomSchemaCapture.CaseSensitivity.CONVERT_TO_LOWER;

public class DataParseContext {
    private final DataParseConfig dataParseConfig;
    private DruidDataSource druidDataSource;
    private BinLogConnector binLogConnector;
    private CustomSchemaCapture schemaCapture;
    private MaxwellBinlogReplicator maxwellBinlogReplicator;
    private Schema schema;


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

    public Schema getSchema() {
        return schema;
    }

    public CustomSchemaCapture getSchemaCapture() {
        return schemaCapture;
    }

     public DataParseContext(DataParseConfig dataParseConfig) {
        this.dataParseConfig = dataParseConfig;
        setup();
    }

    public void setup() {
        setupDataSource();
        setupSchemaCapture();
        setupBinlogConnect();
        setupBinlogReplicator();

    }

    public void setupSchemaCapture() {
        try {
            Connection connection = druidDataSource.getConnection().getConnection();
            schemaCapture = new CustomSchemaCapture(connection, CONVERT_TO_LOWER);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
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

    public void setupBinlogConnect() {
        binLogConnector = new BinLogConnector();
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG, EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY);
        binLogConnector.setEventDeserializer(eventDeserializer);
        CustomEventListener customEventListener = new CustomEventListener(new LinkedBlockingDeque<>(1 << 20), new LinkedHashMap<>());
        binLogConnector.registerEventListener(customEventListener);
    }

    public void setupBinlogReplicator(){
        LinkedBlockingDeque<Event> blockingDeque = binLogConnector.getEventListener().getBlockingDeque();
        maxwellBinlogReplicator = new MaxwellBinlogReplicator(blockingDeque, schema, schemaCapture);
    }
}
