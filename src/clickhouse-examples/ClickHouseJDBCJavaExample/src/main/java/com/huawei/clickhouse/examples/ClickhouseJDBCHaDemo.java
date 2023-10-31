package com.huawei.clickhouse.examples;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDataSource;
import com.clickhouse.jdbc.ClickHouseStatement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;



/**
 * 功能描述 Balance HA Demo
 *
 * @since 2021-06-10
 */
public class ClickhouseJDBCHaDemo {
    private static final Logger LOG = LogManager.getLogger(ClickhouseJDBCHaDemo.class);

    private static final String JDBC_PREFIX = "jdbc:ch://";

    private static ClickHouseDataSource balancedClickhouseDataSource;


    public void initConnection() throws Exception {
        Properties properties = new Properties();
        String proPath = System.getProperty("user.dir") + File.separator + "conf"
                + File.separator + "clickhouse-example.properties";
        try {
            properties.load(new FileInputStream(proPath));
        } catch (IOException e) {
            LOG.error("Failed to load properties file.");
            throw e;
        }
        Properties clickHouseProperties = new Properties();
        boolean isSec = Boolean.parseBoolean(properties.getProperty("CLICKHOUSE_SECURITY_ENABLED"));
        String UriList = properties.getProperty("clickhouse_dataSource_ip_list");
        String userName = properties.getProperty("user");
        boolean isMachineUser = Boolean.parseBoolean(properties.getProperty("isMachineUser"));

        if (isSec) {
            if (isMachineUser) {
                clickHouseProperties.setProperty("isMachineUser", "true");
                clickHouseProperties.setProperty("keytabPath", System.getProperty("user.dir") + File.separator + "conf" + File.separator + "user.keytab");
            }
            String userPass = properties.getProperty("password");
            clickHouseProperties.setProperty(ClickHouseDefaults.PASSWORD.getKey(), userPass);
            clickHouseProperties.setProperty(ClickHouseClientOption.SSL.getKey(), Boolean.toString(true));
            clickHouseProperties.setProperty(ClickHouseClientOption.SSL_MODE.getKey(), "none");
        }
        clickHouseProperties.setProperty(ClickHouseDefaults.USER.getKey(), userName);

        try {
            clickHouseProperties.setProperty(ClickHouseClientOption.FAILOVER.getKey(), "21");
            clickHouseProperties.setProperty(ClickHouseClientOption.LOAD_BALANCING_POLICY.getKey(), "roundRobin");
            balancedClickhouseDataSource = new ClickHouseDataSource(JDBC_PREFIX + UriList, clickHouseProperties);

        } catch (Exception e) {
            LOG.error("Failed to create balancedClickHouseProperties.");
            throw e;
        }
    }

    public void queryData(String databaseName, String tableName) {
        String querySql1 = "select name,age from " + databaseName + "." + tableName + "_all" + " order by age limit 10";
        try (ClickHouseConnection connection = balancedClickhouseDataSource.getConnection();
             ClickHouseStatement statement = connection.createStatement();
             ResultSet set = statement.executeQuery(querySql1)){
             while (set.next()) {
                 LOG.info("Name is: " + set.getString(1) + ", age is: " + set.getString(2));
             }
        } catch (SQLException e) {
            //如果返回码是210则可能为LB服务器Done掉，重新检测连接是否可用，并重试
            if (e.getErrorCode() == 210) {
                try (ClickHouseConnection con = balancedClickhouseDataSource.getConnection();
                     ClickHouseStatement st = con.createStatement();
                     ResultSet rs = st.executeQuery(querySql1)) {
                    while (rs.next()) {
                        LOG.info("Name is: " + rs.getString(1) + ", age is: " + rs.getString(2));
                    }
                } catch (SQLException exception) {
                    LOG.error("Query data failed.");
                }
            } else {
                LOG.error("Query data failed.", e);
            }
        }
    }
}
