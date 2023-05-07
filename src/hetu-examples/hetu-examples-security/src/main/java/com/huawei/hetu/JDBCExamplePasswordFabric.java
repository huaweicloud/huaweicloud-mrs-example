/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.hetu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The example code to connect hetu jdbc server and execute sql statement
 *
 * @since 2022-01-17
 */
public class JDBCExamplePasswordFabric {
    private static Properties properties = new Properties();

    private static void init() throws ClassNotFoundException {
        properties.setProperty("user", "YourUserName"); // need to change the value based on the cluster information
        properties.setProperty("password", "YourPassword"); // need to change the value based on the cluster information
        Class.forName("io.trino.jdbc.TrinoDriver");
    }

    public static void main(String[] args) {
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement statement = null;
        String url = "jdbc:trino://192.168.1.130:29902,192.168.1.131:29902,192.168.1.132:29902/hive/default?serviceDiscoveryMode=hsfabric"; // need to change the value based on the cluster information
        try {
            init();
            String sql = "show tables";
            connection = DriverManager.getConnection(url, properties);
            statement = connection.prepareStatement(sql.trim());
            resultSet = statement.executeQuery();
            int colNum = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= colNum; i++) {
                    System.out.println(resultSet.getString(i) + "\t");
                }
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }
}
