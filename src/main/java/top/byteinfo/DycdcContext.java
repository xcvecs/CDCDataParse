package top.byteinfo;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import top.byteinfo.binlog.DataEventListener;
import top.byteinfo.connector.DycdcConnector;
import top.byteinfo.producer.AbstractProducer;
import top.byteinfo.producer.StdoutProducer;
import top.byteinfo.schema.Schema;
import top.byteinfo.schema.SchemaCapture;
import top.byteinfo.schema.ServerCaseSensitivity;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

public class DycdcContext {


    protected DycdcConfig dycdcConfig;

    protected DataSource dataSource;

    private SchemaCapture schemaCapture;

    private  DycdcBinlogReplicator dycdcBinlogReplicator;

    private   Schema schema;

    private AbstractProducer producer;


    // todo
    //  doc
    public DycdcContext(DycdcConfig dycdcConfig) {
        this.dycdcConfig = dycdcConfig;
        setup();
        parsePre();
    }

    private void parsePre() {
    }

    private void setup() {


        Properties properties = dycdcConfig.getProperties();
        final HikariConfig config = new HikariConfig();
        config.setPoolName("dataSourcePool");
        config.setDriverClassName(properties.getProperty("dataSource.setDriverClassName"));
        config.setUsername(properties.getProperty("dataSource.setUsername"));
        config.setPassword(properties.getProperty("dataSource.setPassword"));
        config.setJdbcUrl(properties.getProperty("dataSource.setJdbcUrl"));
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(3);
        dataSource = new HikariDataSource(config);

        try {
            Connection connection = dataSource.getConnection();
            ServerCaseSensitivity caseSensitivity = DataBaseServerStatus.MaxwellMysqlStatus.captureCaseSensitivity(connection);
            SchemaCapture capture = new SchemaCapture(connection, caseSensitivity);

            this.schemaCapture = capture;

            //todo
            // BoundaryConditionException
            this.schema = capture.capture();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        DycdcConnector dycdcConnector = new DycdcConnector();
        EventDeserializer eventDeserializer = new EventDeserializer();
        eventDeserializer.setCompatibilityMode(
                EventDeserializer.CompatibilityMode.DATE_AND_TIME_AS_LONG,
                EventDeserializer.CompatibilityMode.CHAR_AND_BINARY_AS_BYTE_ARRAY
        );
        String capacity = dycdcConfig.getProperties().getProperty("binLogConnector.registerEventListener");
        DataEventListener dataEventListener = new DataEventListener(new LinkedBlockingDeque<>(Integer.parseInt(capacity)), new LinkedHashMap<>());

        dycdcConnector.setEventDeserializer(eventDeserializer);
        dycdcConnector.registerEventListener(dataEventListener);


        LinkedBlockingDeque<Event> deque = dycdcConnector.getEventListener().getBlockingDeque();

        dycdcBinlogReplicator= new DycdcBinlogReplicator(dycdcConnector,deque,schema,schemaCapture,this);

        this.producer = new StdoutProducer(this);

    }

    public DycdcConfig getDycdcConfig() {
        return dycdcConfig;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public SchemaCapture getSchemaCapture() {
        return schemaCapture;
    }

    public DycdcBinlogReplicator getDycdcBinlogReplicator() {
        return dycdcBinlogReplicator;
    }

    public Schema getSchema() {
        return schema;
    }

    public AbstractProducer getProducer() {
        return producer;
    }
}
