/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
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
 * @since 2019-12-01
 */
public class JDBCExampleBroker {
    private static Properties properties = new Properties();

    private static void init() throws ClassNotFoundException {
        properties.setProperty("user", "YourUserName"); // need to change the value based on the cluster information
        properties.setProperty("password", "YourPassword"); // need to change the value based on the cluster information
        Class.forName("io.prestosql.jdbc.PrestoDriver");
    }

    public static void main(String[] args) {
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement statement = null;
        String url = "jdbc:presto://192.168.1.130:29860,192.168.1.131:29860,192.168.1.132:29860/hive/default?serviceDiscoveryMode=hsbroker"; // need to change the value based on the cluster information
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
