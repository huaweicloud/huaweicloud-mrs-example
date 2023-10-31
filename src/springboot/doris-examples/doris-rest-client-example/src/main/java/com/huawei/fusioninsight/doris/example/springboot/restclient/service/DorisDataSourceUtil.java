/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

public class DorisDataSourceUtil {
    private static final Logger logger = LogManager.getLogger(DorisDataSourceUtil.class);
    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL_PATTERN = "jdbc:mariadb://%s:%d?rewriteBatchedStatements=true";
    private static final String HOST = "192.168.67.78"; // Leader Node host
    private static final int PORT = 29982;   // query_port of Leader Node
    // doris用户
    private static final String USER = "root";
    // doris用户密码
    private static final String PASSWD = "";

    private static String FOUR_EMPTY = "    ";


    public static void main(String[] args) {

    }

    public static Connection createConnection() throws Exception {
        Connection connection = null;
        try {
            Class.forName(JDBC_DRIVER);
            String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT);
            connection = DriverManager.getConnection(dbUrl, USER, PASSWD);
        } catch (Exception e) {
            logger.error("Init doris connection failed.", e);
            throw new Exception(e);
        }
        return connection;
    }

    public static void execDDL(Connection connection, String sql) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        } catch (Exception e) {
            logger.error("Execute sql {} failed.", sql, e);
            throw new Exception(e);
        }
    }

    public static void insert(Connection connection, String sql) throws Exception {
        int INSERT_BATCH_SIZE = 10;
        try(PreparedStatement stmt = connection.prepareStatement(sql)) {

            for (int i =0; i < INSERT_BATCH_SIZE; i++) {
                stmt.setInt(1, i);
                stmt.setInt(2, i * 10);
                stmt.setString(3, String.valueOf(i * 100));
                stmt.addBatch();
            }

            stmt.executeBatch();
        } catch (Exception e) {
            logger.error("Execute sql {} failed.", sql, e);
            throw new Exception(e);
        }
    }

    public static String executeQuery(Connection connection, String sql) throws Exception {

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            // 输出查询的列名到控制台
            ResultSetMetaData resultMetaData = resultSet.getMetaData();
            int columnCount = resultMetaData.getColumnCount();
            StringBuilder resultMsg = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                resultMsg.append(resultMetaData.getColumnLabel(i)).append(FOUR_EMPTY);
            }

            // 输出查询结果到控制台
            StringBuilder result = new StringBuilder();
            String separator = System.getProperty("line.separator");
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    result.append(resultSet.getString(i)).append(FOUR_EMPTY);
                }
                result.append(separator);
            }
            return resultMsg + separator + result;
        } catch (Exception e) {
            logger.error("Execute sql {} failed.", sql, e);
            throw new Exception(e);
        }
    }
}
