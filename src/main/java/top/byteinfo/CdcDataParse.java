package top.byteinfo;


import lombok.extern.slf4j.Slf4j;
import top.byteinfo.iter.DataParseConfig;
import top.byteinfo.iter.DataParseContext;
import top.byteinfo.iter.MaxwellBinlogReplicator;
import top.byteinfo.iter.connect.BinLogConnector;
import top.byteinfo.iter.schema.SchemaCapture;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@Slf4j
public class CdcDataParse implements Runnable {
    Logger logger = Logger.getLogger(CdcDataParse.class.getName());

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






    @Override
    public void run() {

        /**
         * 异步解析数据
         */
        log.info("异步解析数据");
        executorService.submit(maxwellBinlogReplicator);
    }




}
