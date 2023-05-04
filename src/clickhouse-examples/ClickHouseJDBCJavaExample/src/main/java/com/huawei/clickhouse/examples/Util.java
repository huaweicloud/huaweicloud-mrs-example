package com.huawei.clickhouse.examples;

/**
 * 功能描述
 *
 * @since 2021-03-30
 */

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;


public class Util {
    private static Logger log = LogManager.getLogger(Util.class);

    private static final String JDBC_PREFIX = "jdbc:clickhouse://";

    private static final String ERROR_CODE = "999";

    List<List<List<String>>> exeSql(ArrayList<String> sqlList) throws Exception {
        List<List<List<String>>> multiSqlResults = new ArrayList<>();
        for (String sql : sqlList) {
            List<List<String>> singleSqlResult = exeSql(sql);
            multiSqlResults.add(singleSqlResult);
        }
        return multiSqlResults;
    }

    List<List<String>> exeSql(String sql) throws Exception {
        List<List<String>> resultArrayList = new ArrayList<>();
        List<String> serverList = Demo.ckLbServerList;
        String user = Demo.user;
        String password = Demo.password;
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
            clickHouseProperties.setSocketTimeout(60000);
            if (Demo.isSec && Demo.isMachineUser) {
                clickHouseProperties.setMachineUser(true);
                clickHouseProperties.setMachineUserKeytabPath(System.getProperty("user.dir") + File.separator + "conf" + File.separator + "user.keytab");
            }
            if (Demo.sslUsed) {
                clickHouseProperties.setSsl(true);
                clickHouseProperties.setSslMode("none");
            }
            for (int tries = 1; tries <= serverList.size(); tries++) {
                try {
                    resultArrayList = exeSqlNow(sql, serverList.get(tries - 1), clickHouseProperties, user, password);
                } catch (Exception e) {
                    log.info("this is the {}s try.", tries);
                    // 对session过期等与zookeeper连接异常的场景，即返回错误码为999时，进行重试，如果成功则继续；如果失败，则再继续重连致1分钟。
                    String[] errCode = e.toString().split("Code: ");
                    String code = errCode[1].substring(0, errCode[1].indexOf("."));
                    if (ERROR_CODE.equals(code)) {
                        log.info("The error code is 999. Reconnection is required.");
                        long begin = System.currentTimeMillis();
                        long end = System.currentTimeMillis();
                        while (end - begin < 60000) {
                            try {
                                resultArrayList = exeSqlNow(sql, serverList.get(tries - 1), clickHouseProperties, user, password);
                                log.info("Retry succeeded.");
                                break;
                            } catch (Exception exception) {
                                end = System.currentTimeMillis();
                                log.info("Used Time is the {} ms.", end - begin);
                            }
                        }
                    } else {
                        if (tries == serverList.size()) {
                            throw e;
                        }
                    }
                    continue;
                }
                break;
            }
        } catch (Exception e) {
            log.error(e.toString());
            throw e;
        }
        return resultArrayList;
    }

    List<List<String>> exeSqlNow(String sql, String server, ClickHouseProperties clickHouseProperties, String user, String password) throws Exception {
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(JDBC_PREFIX + server, clickHouseProperties);
        List<List<String>> resArrayList = new ArrayList<>();
        long begin = System.currentTimeMillis();
        try (Connection connection = clickHouseDataSource.getConnection(user, password);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            log.info("Current load balancer is {}", server);
            log.info("Execute query:{}", sql);
            long end = System.currentTimeMillis();
            log.info("Execute time is {} ms", end - begin);
            if (null != resultSet && null != resultSet.getMetaData()) {
                int columnCount = resultSet.getMetaData().getColumnCount();
                List<String> rowResultArray = new ArrayList<String>();
                for (int j = 1; j <= columnCount; j++) {
                    rowResultArray.add(resultSet.getMetaData().getColumnName(j));
                }
                resArrayList.add(rowResultArray);
                while (resultSet.next()) {
                    rowResultArray = new ArrayList<String>();
                    for (int j = 1; j <= columnCount; j++) {
                        rowResultArray.add(resultSet.getString(j));
                    }
                    if (rowResultArray.size() != 0) {
                        resArrayList.add(rowResultArray);
                    }
                }
            }
        } catch (Exception e) {
            throw new SQLException("Sql exec failed.");
        }
        return resArrayList;
    }

    void insertData(String databaseName, String tableName, int batchNum, int batchRows) throws Exception {
        List<String> serverList = Demo.ckLbServerList;
        String user = Demo.user;
        String password = Demo.password;
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
            clickHouseProperties.setSocketTimeout(60000);
            if (Demo.isSec && Demo.isMachineUser) {
                clickHouseProperties.setMachineUser(true);
                clickHouseProperties.setMachineUserKeytabPath(System.getProperty("user.dir") + File.separator + "conf" + File.separator + "user.keytab");
            }
            if (Demo.sslUsed) {
                clickHouseProperties.setSsl(true);
                clickHouseProperties.setSslMode("none");
            }
            for (int tries = 1; tries <= serverList.size(); tries++) {
                try {
                    runInsertData(serverList.get(tries - 1), clickHouseProperties, user, password, databaseName, tableName, batchNum, batchRows);
                } catch (Exception e) {
                    log.warn(e.toString());
                    log.info("this is the {}s try.", tries);
                    // 对session过期等与zookeeper连接异常的场景，即返回错误码为999时，进行重试，如果成功则继续；如果失败，则再继续重连致1分钟。
                    String[] errCode = e.toString().split("Code: ");
                    String code = errCode[1].substring(0, errCode[1].indexOf("."));
                    if (ERROR_CODE.equals(code)) {
                        log.info("The error code is 999. Reconnection is required.");
                        long begin = System.currentTimeMillis();
                        long end = System.currentTimeMillis();
                        while (end - begin < 60000) {
                            try {
                                runInsertData(serverList.get(tries - 1), clickHouseProperties, user, password, databaseName, tableName, batchNum, batchRows);
                                log.info("Retry succeeded.");
                                break;
                            } catch (Exception exception) {
                                end = System.currentTimeMillis();
                                log.info("Used Time is the {} ms.", end - begin);
                            }
                        }
                    } else {
                        if (tries == serverList.size()) {
                            throw e;
                        }
                    }
                    continue;
                }
                break;
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw e;
        }
    }

    private void runInsertData(String server, ClickHouseProperties clickHouseProperties, String user, String password, String databaseName, String tableName, int batchNum, int batchRows) throws Exception {
        if (databaseName == null || tableName == null) {
            log.error("Database or table is empty.");
            return;
        } else {
            Pattern pt = Pattern.compile("^[0-9a-zA-Z_]+$");
            Matcher databaseMatch = pt.matcher(databaseName);
            Matcher tableMatch = pt.matcher(tableName);
            if(!databaseMatch.matches() || !tableMatch.matches()) {
                log.error("Database or table input format is incorrect.");
                return;
            }
        }
        ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(JDBC_PREFIX + server, clickHouseProperties);
        String insertSql = "insert into " + databaseName + "." + tableName + " values (?,?,?)";
        try (Connection connection = clickHouseDataSource.getConnection(user, password);
             PreparedStatement preparedStatement = connection.prepareStatement(insertSql)) {
            log.info("Current load balancer is {}", server);
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
            log.error(e.getMessage());
            throw new SQLException("Insert exec failed.");
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
