package com.huawei.clickhouse.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.http.conn.HttpHostConnectException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ru.yandex.clickhouse.BalancedClickhouseDataSource;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.except.ClickHouseErrorCode;
import ru.yandex.clickhouse.except.ClickHouseException;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

/**
 * 功能描述 Balance HA Demo
 *
 * @since 2021-06-10
 */
public class ClickhouseJDBCHaDemo {
    private static final Logger LOG = LogManager.getLogger(ClickhouseJDBCHaDemo.class);

    private static final String JDBC_PREFIX = "jdbc:clickhouse://";

    private static BalancedClickhouseDataSource balancedClickhouseDataSource;


    public void initConnection() throws IOException {
        Properties properties = new Properties();
        String proPath = System.getProperty("user.dir") + File.separator + "conf"
                + File.separator + "clickhouse-example.properties";
        try {
            properties.load(new FileInputStream(proPath));
        } catch (IOException e) {
            LOG.error("Failed to load properties file.");
            throw e;
        }
        ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
        boolean isSec = Boolean.parseBoolean(properties.getProperty("CLICKHOUSE_SECURITY_ENABLED"));
        String UriList = properties.getProperty("clickhouse_dataSource_ip_list");
        String userName = properties.getProperty("user");
        if (isSec) {
            String userPass = properties.getProperty("password");
            clickHouseProperties.setPassword(userPass);
            clickHouseProperties.setSsl(true);
            clickHouseProperties.setSslMode("none");
        }
        clickHouseProperties.setUser(userName);
        //是否在初始化连接时检测连接可用
        clickHouseProperties.setCheckConnection(true);
        balancedClickhouseDataSource = new BalancedClickhouseDataSource(JDBC_PREFIX + UriList, clickHouseProperties)
                //此方法在后台会定时检测连接是否可用，并维护一个可用连接的list，以下配置代表每30s进行一次检测
                .scheduleActualization(30, TimeUnit.SECONDS);
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
            if (e.getErrorCode() == ClickHouseErrorCode.NETWORK_ERROR.code) {
                balancedClickhouseDataSource.actualize();
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
