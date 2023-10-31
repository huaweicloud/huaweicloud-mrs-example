/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.clickhouse.examples;


/**
 * 功能描述
 * exeSql函数中，通过判断transactionSupport和autoCommit，来执行事务性sql，使用的是Statement；
 * insertData函数中，通过判断transactionSupport和autoCommit，来执行事务性插入，使用的是PreparedStatement；
 * 还有一种使用Statement，通过sql=“Begin Transaction; insert ***; commit;”或者sql=“set implicit_transaction=true; insert ***;"方式执行事务sql
 * @since 2021-03-30
 */
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.jdbc.ClickHouseDataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        ResultSet resultSet = null;
        String user = Demo.user;
        String password = Demo.password;
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.CONNECTION_TIMEOUT.getKey(), "60000");
        if (Demo.useTransaction) {
            props.setProperty("transactionSupport", "true");
            if (!Demo.autoCommit) {
                props.setProperty("autoCommit", "false");
            }
        }
        if (Demo.sslUsed) {
            props.setProperty(ClickHouseClientOption.SSL.getKey(), "true");
            props.setProperty(ClickHouseClientOption.SSL_MODE.getKey(), "none");
        }
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            for (int tries = 1; tries <= serverList.size(); tries++) {
                try {
                    log.info("Current load balancer is {}", serverList.get(tries - 1));
                    ClickHouseDataSource clickHouseDataSource =
                            new ClickHouseDataSource(JDBC_PREFIX + serverList.get(tries - 1), props);
                    connection = clickHouseDataSource.getConnection(user, password);
                    statement = connection.createStatement();
                    log.info("Execute query:{}", sql);
                    resultSet = statement.executeQuery(sql);
                    // 事务手动提交
                    if (Demo.useTransaction && !Demo.autoCommit) {
                        connection.commit();
                    }

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
                    // 事务手动回滚
                    if (Demo.useTransaction && !Demo.autoCommit) {
                        log.error("Need to rollback");
                        connection.rollback();
                    }
                    if (tries == serverList.size()) {
                        log.error("All tries is used ");
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
                if (resultSet != null) {
                    resultSet.close();
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
        Properties props = new Properties();
        props.setProperty(ClickHouseClientOption.SOCKET_TIMEOUT.getKey(), "60000");
        if (Demo.useTransaction) {
            props.setProperty("transactionSupport", "true");
            if (!Demo.autoCommit) {
                props.setProperty("autoCommit", "false");
            }
        }
        if (Demo.sslUsed) {
            props.setProperty(ClickHouseClientOption.SSL.getKey(), "true");
            props.setProperty(ClickHouseClientOption.SSL_MODE.getKey(), "none");
        }
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
            for (int tries = 1; tries <= serverList.size(); tries++) {
                try {
                    log.info("Current load balancer is {}", serverList.get(tries - 1));
                    ClickHouseDataSource clickHouseDataSource =
                            new ClickHouseDataSource(JDBC_PREFIX + serverList.get(tries - 1), props);
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
                        preparedStatement.executeBatch();
                        // 事务手动提交
                        if (Demo.useTransaction && !Demo.autoCommit) {
                            connection.commit();
                        }
                        Thread.sleep(1500);
                    }
                    long allBatchEnd = System.currentTimeMillis();
                    log.info("Inert all batch time is {} ms", allBatchEnd - allBatchBegin);
                } catch (Exception e) {
                    log.warn(e.toString());
                    log.info("this is the {}s try.", tries);
                    // 事务手动回滚
                    if (Demo.useTransaction && !Demo.autoCommit) {
                        log.error("Need to rollback");
                        connection.rollback();
                    }
                    if (tries == serverList.size()) {
                        log.error("All tries is used ");
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
            java.util.Date start = format.parse(beginDate); // 构造开始日期
            java.util.Date end = format.parse(endDate); // 构造结束日期
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
        if (rtn == begin || rtn == end) {
            return random(begin, end);
        }
        return rtn;
    }
}
