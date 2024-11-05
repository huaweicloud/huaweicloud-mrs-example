/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

public class DorisDataSourceUtil {
    private static final Logger logger = LogManager.getLogger(DorisDataSourceUtil.class);
    private static final String DB_URL_PATTERN = "jdbc:mariadb://%s:%d?rewriteBatchedStatements=true";
    private static String HOST = ""; // Leader Node host
    private static long PORT;   // query_port of Leader Node
    // 运行本示例前请先在本地环境变量中设置环境变量DORIS_MY_USER和DORIS_MY_PASSWORD。建议密文存放，使用时解密，确保安全。
    private static String USER = "";
    private static String PASSWD = "";

    private static String FOUR_EMPTY = "    ";


    public static void main(String[] args) {

    }

    public static Connection createConnection() throws Exception {
        Connection connection = null;
        try {
            Properties properties = new Properties();
            // 使用ClassLoader加载properties配置文件生成对应的输入流
            InputStream in = DorisDataSourceUtil.class.getClassLoader().getResourceAsStream("conf.properties");
            // 使用properties对象加载输入流
            properties.load(in);
            //获取key对应的value值
            USER = properties.getProperty("USER");
            PASSWD = properties.getProperty("PASSWD");
            HOST = properties.getProperty("HOST");
            PORT = Long.parseLong(properties.getProperty("QUERY_PORT"));
            Class.forName(properties.getProperty("JDBC_DRIVER"));
            String dbUrl = String.format(DB_URL_PATTERN, HOST, PORT);
            connection = DriverManager.getConnection(dbUrl, USER, PASSWD == null || PASSWD.equals("") ? "" : PASSWD);
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
