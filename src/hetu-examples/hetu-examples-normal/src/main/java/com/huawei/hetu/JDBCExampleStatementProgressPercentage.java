/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.hetu;

import io.trino.jdbc.TrinoResultSet;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class JDBCExampleStatementProgressPercentage
{

    private static Properties properties = new Properties();
    public static Connection connection = null;
    public static ResultSet result = null;
    public static PreparedStatement statement = null;

    private static void init()
            throws ClassNotFoundException
    {
        properties.setProperty("user", "YourUserName"); // need to change the value based on the cluster information
        properties.setProperty("SSL", "false");
        properties.setProperty("tenant", "YourTenant");
        Class.forName("io.trino.jdbc.TrinoDriver");
    }

    /**
     * Program entry
     *
     * @param args no need program parameter
     */
    public static void main(String[] args)
    {
        String url = "jdbc:trino://192.168.1.130:29861/hive/default?serviceDiscoveryMode=hsbroker"; // the ip address is the ip address of hsbroker, need to change the ip value based on the cluster information
        try {
            init();
            String sql = "show tables";
            connection = DriverManager.getConnection(url, properties);
            statement = connection.prepareStatement(sql.trim());
            result = statement.executeQuery();

            TrinoResultSet rs = (TrinoResultSet) result;
            new Thread()
            {
                public void run()
                {
                    Timer timer = new Timer();
                    // start executing after 3 seconds and execute every 2 seconds
                    timer.schedule(new TimerTask()
                    {
                        @Override
                        public void run()
                        {
                            double statementProgressPercentage = rs.getProgressPercentage().orElse(0.0);
                            System.out.println("The Current Query Progress Percentage is " + statementProgressPercentage * 100 + "%");
                            if ("FINISHED".equals(rs.getStatementStatus().orElse(""))) {
                                System.out.println("The Current Query Progress Percentage is 100%");
                                timer.cancel();
                                Thread.currentThread().interrupt();
                            }
                        }
                    }, 3000, 2000);
                }
            }.start();

            ResultSetMetaData resultMetaData = result.getMetaData();
            int colNum = resultMetaData.getColumnCount();
            for (int j = 1; j <= colNum; j++) {
                try {
                    System.out.print(resultMetaData.getColumnLabel(j) + "\t");
                }
                catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }

            while (result.next()) {
                for (int j = 1; j <= colNum; j++) {
                    System.out.print(result.getString(j) + "\t");
                }
                System.out.println();
            }
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        finally {
            if (result != null) {
                try {
                    result.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}