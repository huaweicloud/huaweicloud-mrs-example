/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2019. All rights reserved.
 */

package com.huawei.hetu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;

/**
 * The example code to connect hetu jdbc server and execute sql statement
 *
 * @since 2019-12-01
 */
public class JDBCExampleZk {
    private static Properties properties = new Properties();

    private static void init() throws ClassNotFoundException {
        properties.setProperty("user", "YourUserName"); // need to change the value based on the cluster information
        properties.setProperty("tenant", "YourTenant"); // need to change the value based on the cluster information
        properties.setProperty("deploymentMode", "on_yarn");
        properties.setProperty("ZooKeeperAuthType", "simple");
        Class.forName("io.trino.jdbc.TrinoDriver");
    }

    /**
     * Program entry
     *
     * @param args no need program parameter
     */
    public static void main(String[] args) {
        Connection connection = null;
        ResultSet result = null;
        PreparedStatement statement = null;
        String url = "jdbc:trino://192.168.1.130:24002,192.168.1.131:24002,192.168.1.132:24002/hive/default?"
                + "serviceDiscoveryMode=zooKeeper&zooKeeperNamespace=hsbroker"; // need to change the value based on the cluster information

        try {
            init();

            String sql = "show tables";
            connection = DriverManager.getConnection(url, properties);
            statement = connection.prepareStatement(sql.trim());
            result = statement.executeQuery();
            ResultSetMetaData resultMetaData = result.getMetaData();
            int colNum = resultMetaData.getColumnCount();
            for (int j = 1; j <= colNum; j++) {
                System.out.print(resultMetaData.getColumnLabel(j) + "\t");
            }
            System.out.println();
            while (result.next()) {
                for (int j = 1; j <= colNum; j++) {
                    System.out.print(result.getString(j) + "\t");
                }
                System.out.println();
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (result != null) {
                try {
                    result.close();
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
