package top.byteinfo;


import top.byteinfo.iter.CustomSchemaCapture;
import top.byteinfo.iter.DataParseConfig;
import top.byteinfo.iter.DataParseContext;
import top.byteinfo.iter.MaxwellBinlogReplicator;
import top.byteinfo.iter.connect.BinLogConnector;
import top.byteinfo.source.maxwell.schema.Schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class CDCDataParse implements Closeable, Runnable {

    public static final Logger logger = Logger.getLogger(CDCDataParse.class.getName());
    private final static ExecutorService executorService = Executors.newFixedThreadPool(5);


    private final BinLogConnector binLogConnector;
    private final CustomSchemaCapture schemaCapture;
    private final MaxwellBinlogReplicator maxwellBinlogReplicator;

    private DataParseContext context;


    private Schema schema;


    public CDCDataParse() {
        this(new DataParseConfig());
    }

    public CDCDataParse(DataParseContext dataParseContext) {
        this.context = dataParseContext;
        this.schemaCapture = context.getSchemaCapture();
        this.binLogConnector = context.getBinLogConnector();
        this.maxwellBinlogReplicator = context.getMaxwellBinlogReplicator();
    }

    public CDCDataParse(DataParseConfig dataParseConfig) {
        this(new DataParseContext(dataParseConfig));


    }


    public static void main(String[] args) {
        CDCDataParse cdcDataParse = new CDCDataParse();
        System.out.println("t");
        cdcDataParse.run();
        System.out.println("t");
    }


    /**
     *
     */
    public Schema dataModelCapture() {
        try {
//            CustomSchemaCapture schemaCapture = context.getSchemaCapture();
            Schema schema = schemaCapture.capture();
            return schema;
        } catch (Exception e) {
            System.out.println(e);
        }

        return null;
    }


    @Override
    public void run() {
        /**
         *
         */
        schema = dataModelCapture();


        /**
         * 全局锁。
         */
        dbGlobalLock();
        long start = System.currentTimeMillis();
        System.out.println("start:" + start);
        System.out.println();
        executorService.submit(binLogConnector);// todo 添加标志位
        dataModelCapture();// todo 添加标志位
        long end = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println("end:" + end);
        dbGlobalUNLock();

        /**
         * parse data
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