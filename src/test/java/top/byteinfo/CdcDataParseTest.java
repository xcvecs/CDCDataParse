package top.byteinfo;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.concurrent.Executors;

public class CdcDataParseTest {
    public static void main(String[] args) {
        CdcDataParse cdcDataParse = new CdcDataParse();
        Executors.newSingleThreadExecutor().submit(cdcDataParse);
    }
    @Test
    public void demo(){

        LinkedHashMap<String,String> urowChanged0 = new LinkedHashMap<>();
        LinkedHashMap<String,String> urowChanged1 = new LinkedHashMap<>();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            String s = sb.append(i).append("s").toString();
            urowChanged0.put(i+"key",s);
            urowChanged1.put(i+"value",s);
        }
        String s = new StringBuilder("old:").append(urowChanged0).append("\n").append("new:").append(urowChanged1).toString();
        System.out.println(urowChanged0+"\n"+urowChanged1);
        System.out.println(s);
    }
}
