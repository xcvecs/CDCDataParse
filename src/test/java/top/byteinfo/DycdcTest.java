package top.byteinfo;

import java.util.concurrent.Executors;

public class DycdcTest {

    public static void main(String[] args) {
        Dycdc dycdc = new Dycdc();

        Executors.newSingleThreadExecutor().submit(dycdc);

    }
}
