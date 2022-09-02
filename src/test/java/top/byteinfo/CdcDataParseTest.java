package top.byteinfo;

import org.junit.jupiter.api.Test;

import java.util.concurrent.Executors;

public class CdcDataParseTest {
    public static void main(String[] args) {
        CdcDataParse cdcDataParse = new CdcDataParse();
        Executors.newSingleThreadExecutor().submit(cdcDataParse);
    }
    @Test
    public void demo(){

    }
}
