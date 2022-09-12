package top.byteinfo.connect;

import com.github.shyiko.mysql.binlog.BinaryLogClient;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Connector {
    private BinaryLogClient binlogClient;
    InetSocketAddress connectedHost = null;

    public void setup() {
        String username = "root";
        String password = "root";
        binlogClient = createBinlogClient(connectedHost.getHostString(), connectedHost.getPort(), username, password);

    }
    BinaryLogClient createBinlogClient(String hostname, int port, String username, String password) {
        return new BinaryLogClient(hostname, port, username, password);
    }

    private List<InetSocketAddress> getHosts(String hostsString) {

        if (hostsString == null) {
            return null;
        }
        final String[] hostsSplit = hostsString.split(",");
        List<InetSocketAddress> hostsList = new ArrayList<>();

        for (String item : hostsSplit) {
            String[] addresses = item.split(":");
            if (addresses.length != 2) {
                throw new ArrayIndexOutOfBoundsException("Not in host:port format");
            }

            hostsList.add(new InetSocketAddress(addresses[0].trim(), Integer.parseInt(addresses[1].trim())));
        }
        return hostsList;
    }
}
