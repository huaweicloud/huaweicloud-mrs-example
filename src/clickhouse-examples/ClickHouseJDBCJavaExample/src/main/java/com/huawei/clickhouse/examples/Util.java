package com.huawei.clickhouse.examples;

/**
 * 功能描述
 *
 * @since 2021-03-30
 */

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseDefaults;
import com.clickhouse.jdbc.ClickHouseDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;



public class Util {
    private static Logger log = LogManager.getLogger(Util.class);

    private static final String JDBC_PREFIX = "jdbc:clickhouse://";

    ArrayList<ArrayList<ArrayList<String>>> exeSql(ArrayList<String> sqlList) throws Exception {
        ArrayList<ArrayList<ArrayList<String>>> multiSqlResults = new ArrayList<ArrayList<ArrayList<String>>>();
        for (String sql : sqlList) {
            ArrayList<ArrayList<String>> singleSqlResult = exeSql(sql);
            multiSqlResults.add(singleSqlResult);
        }
        return multiSqlResults;
    }

    ArrayList<ArrayList<String>> exeSql(String sql) throws Exception {
        ArrayList<ArrayList<String>> resultArrayList = new ArrayList<ArrayList<String>>();
        List<String> serverList = Demo.ckLbServerList;
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet;
        String user = Demo.user;
        String password = Demo.password;
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            Properties clickHouseProperties = new Properties();
            clickHouseProperties.setProperty(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey(), Integer.toString(60000));
            if (Demo.isSec && Demo.isMachineUser) {
                clickHouseProperties.setProperty("isMachineUser", "true");
                clickHouseProperties.setProperty(ClickHouseDefaults.USER.getKey(), user);
                clickHouseProperties.setProperty("keytabPath", System.getProperty("user.dir") + File.separator + "conf" + File.separator + "user.keytab");
            }

            if (Demo.sslUsed) {
                clickHouseProperties.setProperty(ClickHouseClientOption.SSL.getKey(), Boolean.toString(true));
                clickHouseProperties.setProperty(ClickHouseClientOption.SSL_MODE.getKey(), "none");
            }
            for (int tries = 1; tries <= serverList.size(); tries++) {
                try {
                    log.info("Current load balancer is {}", serverList.get(tries - 1));
                    ClickHouseDataSource clickHouseDataSource =
                            new ClickHouseDataSource(JDBC_PREFIX + serverList.get(tries - 1), clickHouseProperties);
                    connection = clickHouseDataSource.getConnection(user, password);
                    statement = connection.createStatement();
                    log.info("Execute query:{}", sql);
                    long begin = System.currentTimeMillis();
                    resultSet = statement.executeQuery(sql);
                    long end = System.currentTimeMillis();
                    log.info("Execute time is {} ms", end - begin);
                    if (null != resultSet && null != resultSet.getMetaData()) {
                        int columnCount = resultSet.getMetaData().getColumnCount();
                        ArrayList<String> rowResultArray = new ArrayList<String>();
                        for (int j = 1; j <= columnCount; j++) {
                            rowResultArray.add(resultSet.getMetaData().getColumnName(j));
                        }
                        resultArrayList.add(rowResultArray);
                        while (resultSet.next()) {
                            rowResultArray = new ArrayList<String>();
                            for (int j = 1; j <= columnCount; j++) {
                                rowResultArray.add(resultSet.getString(j));
                            }
                            if (rowResultArray.size() != 0) {
                                resultArrayList.add(rowResultArray);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn(e.toString());
                    log.info("this is the {}s try.", tries);
                    if (tries == serverList.size()) {
                        throw e;
                    }
                    continue;
                }
                break;
            }
        } catch (Exception e) {
            log.error(e.toString());
            throw e;
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }
        return resultArrayList;
    }

    void insertData(String databaseName, String tableName, int batchNum, int batchRows) throws Exception {
        List<String> serverList = Demo.ckLbServerList;
        Connection connection = null;
        String user = Demo.user;
        String password = Demo.password;
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            Properties clickHouseProperties = new Properties();
            clickHouseProperties.setProperty(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey(), Integer.toString(60000));
            if (Demo.isSec && Demo.isMachineUser) {
                clickHouseProperties.setProperty("isMachineUser", "true");
                clickHouseProperties.setProperty("user", user);
                clickHouseProperties.setProperty("keytabPath", System.getProperty("user.dir") + File.separator + "conf" + File.separator + "user.keytab");
            }
            if (Demo.sslUsed) {
                clickHouseProperties.setProperty(ClickHouseClientOption.SSL.getKey(), Boolean.toString(true));
                clickHouseProperties.setProperty(ClickHouseClientOption.SSL_MODE.getKey(), "none");
            }
            for (int tries = 1; tries <= serverList.size(); tries++) {
                try {
                    log.info("Current load balancer is {}", serverList.get(tries - 1));
                    ClickHouseDataSource clickHouseDataSource =
                            new ClickHouseDataSource(JDBC_PREFIX + serverList.get(tries - 1), clickHouseProperties);
                    connection = clickHouseDataSource.getConnection(user, password);
                    String insertSql = "insert into " + databaseName + "." + tableName + " values (?,?,?)";
                    PreparedStatement preparedStatement = connection.prepareStatement(insertSql);
                    long allBatchBegin = System.currentTimeMillis();
                    for (int j = 0; j < batchNum; j++) {
                        for (int i = 0; i < batchRows; i++) {
                            preparedStatement.setString(1, "huawei_" + (i + j * 10));
                            preparedStatement.setInt(2, ((int) (Math.random() * 100)));
                            preparedStatement.setDate(3, generateRandomDate("2020-01-01", "2021-12-31"));
                            preparedStatement.addBatch();
                        }
                        long begin = System.currentTimeMillis();
                        preparedStatement.executeBatch();
                        long end = System.currentTimeMillis();
                        log.info("Insert batch time is {} ms", end - begin);
                        Thread.sleep(1500);
                    }
                    long allBatchEnd = System.currentTimeMillis();
                    log.info("Inert all batch time is {} ms", allBatchEnd - allBatchBegin);
                } catch (Exception e) {
                    log.warn(e.toString());
                    log.info("this is the {}s try.", tries);
                    if (tries == serverList.size()) {
                        throw e;
                    }
                    continue;
                }
                break;
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw e;
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                log.error(e.getMessage());
            }
        }
    }

    private Date generateRandomDate(String beginDate, String endDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            java.util.Date start = format.parse(beginDate);// 构造开始日期
            java.util.Date end = format.parse(endDate);// 构造结束日期
            // getTime()表示返回自 1970 年 1 月 1 日 00:00:00 GMT 以来此 Date 对象表示的毫秒数。
            if (start.getTime() >= end.getTime()) {
                return null;
            }
            long date = random(start.getTime(), end.getTime());
            return new Date(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static long random(long begin, long end) {
        long rtn = begin + (long) (Math.random() * (end - begin));
        // 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }
}
