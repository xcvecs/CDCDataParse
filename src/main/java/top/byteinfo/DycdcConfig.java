package top.byteinfo;

import java.util.Properties;

public class DycdcConfig {
    private final Properties properties;

    public Properties getProperties() {
        return properties;
    }
    public DycdcConfig() {
        this.properties = new Properties();
        setupDataSourceConfig();
        setupBinlogConnectConfig();
    }

    private void setupDataSourceConfig() {
//        setup dataSource
        properties.setProperty("dataSource.setDriverClassName", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("dataSource.setUsername", "root");
        properties.setProperty("dataSource.setPassword", "root");
        properties.setProperty("dataSource.setJdbcUrl", "jdbc:mysql://localhost?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC");

//        config.setMaximumPoolSize(Integer.parseInt(properties.getProperty("dataSource.setMaximumPoolSize")));
//        config.setMinimumIdle(Integer.parseInt(properties.getProperty("dataSource.setMinimumIdle")));
//        config.setMaxLifetime(Long.parseLong(properties.getProperty("dataSource.setMaxWait")));

        //todo
        properties.setProperty("dataSource.setInitialSize", "5");
        properties.setProperty("dataSource.setMaxActive", "5");
        properties.setProperty("dataSource.setMinIdle", "3");
        properties.setProperty("dataSource.setMaxWait", "3000");
    }



    private void setupBinlogConnectConfig(){
        properties.setProperty("binLogConnector.registerEventListener", String.valueOf(1<<13));
    }
}
