package top.byteinfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Dycdc implements Runnable{

    private static final Logger log= LoggerFactory.getLogger(Dycdc.class);
    protected DycdcConfig dycdcConfig;
    protected DycdcContext dycdcContext;
    protected DycdcBinlogReplicator dycdcBinlogReplicator;

    public Dycdc(){
        this(new DycdcConfig());
    }

    protected Dycdc(DycdcContext dycdcContext){
        this.dycdcConfig= dycdcContext.getDycdcConfig();
        this.dycdcContext=dycdcContext;
    }

    public Dycdc(DycdcConfig dycdcConfig){
        this(new DycdcContext(dycdcConfig));
    }

    @Override
    public void run() {

        start();
    }

    private void start() {
        try {
            this.startInner();
        } catch ( Exception e) {
            ;
        }
    }

    private void startInner() {

        DycdcBinlogReplicator binlogReplicator = dycdcContext.getDycdcBinlogReplicator();
        binlogReplicator.getDycdcConnector().tryConnect();
        binlogReplicator.run();



    }
;

}
