package top.byteinfo.iter;

import java.util.Properties;

public class DataParseConfig {

    private final Properties properties;

    public DataParseConfig() {
        this.properties = new Properties();
        setupDataSource();
    }

    private void setupDataSource() {
//        setup dataSource
        properties.setProperty("dataSource.setDriverClassName", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("dataSource.setUsername", "root");
        properties.setProperty("dataSource.setPassword", "root");
        properties.setProperty("dataSource.setUrl", "jdbc:mysql://localhost?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=UTC");
        properties.setProperty("dataSource.setInitialSize", "5");
        properties.setProperty("dataSource.setMaxActive", "10");
        properties.setProperty("dataSource.setMinIdle", "3");
        properties.setProperty("dataSource.setMaxWait", "3000");
    }



    public Properties getProperties() {
        return properties;
    }
}
