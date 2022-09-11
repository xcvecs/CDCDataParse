package debug;

import com.zaxxer.hikari.HikariDataSource;
import com.zendesk.maxwell.DataEventParse;
import com.zendesk.maxwell.connect.HconnectionPool;
import com.zendesk.maxwell.replication.Position;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;


public class Demo {
    private static final String jdbcurl = "jdbc:mysql://localhost?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC";
    private static final String jdbcurlwith = "jdbc:mysql://localhost/maxwell?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC";


    public static void main(String[] args) {
        HikariDataSource hikariDataSource1 = new HikariDataSource();
        hikariDataSource1.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariDataSource1.setUsername("root");
        hikariDataSource1.setPassword("root");
        hikariDataSource1.setJdbcUrl(jdbcurlwith);
        HikariDataSource hikariDataSource0 = new HikariDataSource();
        hikariDataSource0.setDriverClassName("com.mysql.cj.jdbc.Driver");
        hikariDataSource0.setUsername("root");
        hikariDataSource0.setPassword("root");
        hikariDataSource0.setJdbcUrl(jdbcurl);

        HconnectionPool hconnectionPool1 = new HconnectionPool(hikariDataSource1);
        HconnectionPool hconnectionPool0 = new HconnectionPool(hikariDataSource0);

        List<HconnectionPool> pools = Arrays.asList(hconnectionPool0, hconnectionPool1);

        DataEventParse dataParse = new DataEventParse();
        Position position = dataParse.getPosition(pools.get(1));
        boolean setup = dataParse.setup(pools, position);
        System.out.println();

    }
}
