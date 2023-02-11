package com.huawei.clickhouse.examples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.housepower.jdbc.BalancedClickhouseDataSource;
import com.github.housepower.jdbc.ClickHouseConnection;
import com.github.housepower.jdbc.statement.ClickHouseStatement;
import com.github.housepower.settings.ClickHouseErrCode;



/**
 * 功能描述 Balance HA Demo, Native JDBC 不支持ssl访问
 *
 * @since 2021-06-10
 */
public class NativeJDBCHaDemo {

    private static final Logger LOG = LogManager.getLogger(NativeJDBCHaDemo.class);

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
        String UriList = properties.getProperty("native_dataSource_ip_list");
        //是否在初始化连接时检测连接可用
        properties.put("is_check_connection", "true");
        properties.put("configPath", System.getProperty("user.dir") + File.separator + "conf");
        balancedClickhouseDataSource = new BalancedClickhouseDataSource(JDBC_PREFIX + UriList, properties)
                //此方法在后台会定时检测连接是否可用，并维护一个可用连接的list，以下配置代表每30s进行一次检测
                .scheduleActualization(30, TimeUnit.SECONDS);
    }

    public void queryData(String databaseName, String tableName) {
        String querySql1 = "select name,age from " + databaseName + "." + tableName + "_all" + " order by age limit 10";
        try (ClickHouseConnection connection = balancedClickhouseDataSource.getConnection();
             ClickHouseStatement statement = (ClickHouseStatement) connection.createStatement();
             ResultSet set = statement.executeQuery(querySql1)){
            while (set.next()) {
                LOG.info("Name is: " + set.getString(1) + ", age is: " + set.getString(2));
            }
        } catch (SQLException e) {
            //如果返回码是210则可能为LB服务器Done掉，重新检测连接是否可用，并重试
            if (e.getErrorCode() == ClickHouseErrCode.NETWORK_ERROR.code()) {
                balancedClickhouseDataSource.scheduleActualization(30, TimeUnit.SECONDS);
                try (Connection con = balancedClickhouseDataSource.getConnection();
                     Statement statement = con.createStatement();
                     ResultSet rs = statement.executeQuery(querySql1)) {
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
