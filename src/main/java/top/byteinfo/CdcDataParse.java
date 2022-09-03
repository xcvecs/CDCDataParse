package top.byteinfo;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.byteinfo.iter.DataParseConfig;
import top.byteinfo.iter.DataParseContext;
import top.byteinfo.iter.MaxwellBinlogReplicator;
import top.byteinfo.iter.connect.BinLogConnector;
import top.byteinfo.iter.schema.SchemaCapture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CdcDataParse implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(CdcDataParse.class.getName());

    private final static ExecutorService executorService = Executors.newFixedThreadPool(5);


    private final BinLogConnector binLogConnector;
    private final SchemaCapture schemaCapture;
    private final MaxwellBinlogReplicator maxwellBinlogReplicator;

    private DataParseContext context;


    public CdcDataParse() {
        this(new DataParseConfig());
    }

    public CdcDataParse(DataParseContext dataParseContext) {
        this.context = dataParseContext;
        this.schemaCapture = context.getSchemaCapture();
        this.binLogConnector = context.getBinLogConnector();
        this.maxwellBinlogReplicator = context.getMaxwellBinlogReplicator();
    }

    public CdcDataParse(DataParseConfig dataParseConfig) {
        this(new DataParseContext(dataParseConfig));


    }

    public static void main(String[] args) {
        String[] commands = new String[5];
        commands[0] = "[--debug]".trim();
        commands[1] = "[--port=40000]".trim();
        commands[2] = "[--config=/absolute/path/to/syncerConfig.yml]".trim();
        commands[3] = "--producerConfig=/absolute/path/to/producer.yml".trim();
        commands[4] = "--consumerConfig=/absolute/path/to/consumer1.yml,/absolute/path/to/consumer2.yml".trim();
        for (String arg : args) {
            System.out.println("command line :" + arg);
        }

        for (int i = 0; i < commands.length; i++) {
            if (commands[i].equals(args[i])) System.out.println("" + i + ": true");
        }

    }

    @Override
    public void run() {

        /**
         * 异步解析数据
         */
        log.debug("异步解析数据");
        executorService.submit(maxwellBinlogReplicator);
    }


}
