/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.clickhouse.example.springboot.restclient.impl;

import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.jdbc.ClickHouseDataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * clickhouse springboot样例工具类
 *
 * @since 2022-12-16
 */
public class Util {
    private static final Logger log = LogManager.getLogger(Util.class);

    private static final String JDBC_PREFIX = "jdbc:clickhouse://";

    static Properties clickHouseProperties = new Properties();

    /**
     * 执行一组SQL语句
     *
     * @param sqlList SQL语句列表
     * @return 一组SQL语句的执行结果
     */
    public static List<List<List<String>>> executeQueryList(ArrayList<String> sqlList) throws Exception {
        List<List<List<String>>> multiSqlResults = new ArrayList<>();
        for (String sql : sqlList) {
            List<List<String>> singleSqlResult = executeQuery(sql);
            multiSqlResults.add(singleSqlResult);
        }
        return multiSqlResults;
    }

    private static void initProperties(Properties clickHouseProperties) {
        String user = ClickHouseFunc.user;
        String password = ClickHouseFunc.password == null ? "" : ClickHouseFunc.password;
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        clickHouseProperties.setProperty(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey(), Integer.toString(60000));
        clickHouseProperties.setProperty("user", user);
        clickHouseProperties.setProperty("password", password);
        if (ClickHouseFunc.isSec && ClickHouseFunc.isMachineUser) {
            clickHouseProperties.setProperty("isMachineUser", "true");
            clickHouseProperties.setProperty("user", user);
            clickHouseProperties.setProperty("keytabPath", System.getProperty("user.dir") + File.separator + "conf" + File.separator + "user.keytab");
        }
        if (ClickHouseFunc.sslUsed) {
            clickHouseProperties.setProperty(ClickHouseClientOption.SSL.getKey(), Boolean.toString(true));
            clickHouseProperties.setProperty(ClickHouseClientOption.SSL_MODE.getKey(), "none");
        }
    }

    /**
     * 执行一条SQL语句
     *
     * @param sql SQL语句
     * @return SQL查询结果
     * @throws SQLException
     */
    public static List<List<String>> executeQuery(String sql) throws Exception {
        List<List<String>> resultArrayList = new ArrayList<List<String>>();
        List<String> serverList = ClickHouseFunc.ckLbServerList;
        initProperties(clickHouseProperties);
        int tries = 0;
        while (tries < serverList.size()) {
            String server = serverList.get(tries);
            log.info("Try times is {}, current load balancer is {}.", tries, server);
            boolean querySuccess = execSqlOnServer(sql, server, clickHouseProperties, resultArrayList);
            if (querySuccess) {
                break;
            } else {
                tries++;
                if (tries == serverList.size()) {
                    throw new SQLException("Failed to exec the sql.");
                }
            }
        }
        return resultArrayList;
    }

    private static boolean execSqlOnServer(String sql, String server, Properties clickHouseProperties, List<List<String>> resultArrayList) throws SQLException {
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(JDBC_PREFIX + server, clickHouseProperties);
        ResultSet resultSet = null;
        try (Connection connection = clickHouseDataSource.getConnection((String) clickHouseProperties.get("user"), (String) clickHouseProperties.get("password")); PreparedStatement statement = connection.prepareStatement(sql);) {
            long begin = System.currentTimeMillis();
            resultSet = statement.executeQuery();
            long end = System.currentTimeMillis();
            log.info("Execute sql {}, time is {} ms", sql, end - begin);
            if (null != resultSet && null != resultSet.getMetaData()) {
                parseResultSet(resultArrayList, resultSet);
            }
        } catch (SQLException e) {
            log.warn("Failed to execute sql {}, please check ClickHouse Server log for more information.", sql);
            return false;
        } finally {
            if (null != resultSet) {
                resultSet.close();
            }
        }
        return true;
    }

    private static void parseResultSet(List<List<String>> resultArrayList, ResultSet resultSet) throws SQLException {
        int columnCount = resultSet.getMetaData().getColumnCount();
        while (resultSet.next()) {
            ArrayList<String> rowResultArray = new ArrayList<String>();
            for (int j = 1; j <= columnCount; j++) {
                rowResultArray.add(resultSet.getString(j));
            }
            if (rowResultArray.size() != 0) {
                resultArrayList.add(rowResultArray);
            }
        }
    }

    /**
     * 插入数据到表中
     *
     * @param databaseName 数据库名称
     * @param tableName    表名称
     * @param batchNum     插入的批次总数
     * @param batchRows    每个批次多少行数据
     * @throws Exception SQL执行失败的异常
     */
    public static void insertData(String databaseName, String tableName, int batchNum, int batchRows) throws Exception {
        List<String> serverList = ClickHouseFunc.ckLbServerList;
        initProperties(clickHouseProperties);
        String insertSql = "insert into " + databaseName + "." + tableName + " values (?,?,?)";
        for (int tries = 1; tries <= serverList.size(); tries++) {
            ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(JDBC_PREFIX + serverList.get(tries - 1), clickHouseProperties);
            try (Connection connection = clickHouseDataSource.getConnection((String) clickHouseProperties.get("user"), (String) clickHouseProperties.get("password"));
                 PreparedStatement statement = connection.prepareStatement(insertSql);) {
                long allBatchBegin = System.currentTimeMillis();
                for (int j = 0; j < batchNum; j++) {
                    for (int i = 0; i < batchRows; i++) {
                        statement.setString(1, "huawei_" + (i + j * 10));
                        statement.setInt(2, ((int) (Math.random() * 100)));
                        statement.setDate(3, generateRandomDate("2022-01-01", "2022-12-31"));
                        statement.addBatch();
                    }
                    long begin = System.currentTimeMillis();
                    statement.executeBatch();
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
    }

    private static Date generateRandomDate(String beginDate, String endDate) {
        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            // 构造开始日期
            java.util.Date start = format.parse(beginDate);
            // 构造结束日期
            java.util.Date end = format.parse(endDate);
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

