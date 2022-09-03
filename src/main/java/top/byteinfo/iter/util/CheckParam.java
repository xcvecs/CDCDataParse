package top.byteinfo.iter.util;

import com.github.shyiko.mysql.binlog.event.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
public class CheckParam {
    private static final Logger log = LoggerFactory.getLogger(CheckParam.class);
    //todo
    public static boolean dataEventPreCheck(LinkedList<Event> eventTemp){
        int length = eventTemp.size();
        boolean b = length % 2 == 0 || length % 5 == 0;

        switch (length){
            case 2:
                System.out.println("2");//todo
                break;
            case 5:
                System.out.println("5");//todo
                break;
            default:
                break;
        }
        return true;
    }

    public static void checkOverLog(){

    }
}