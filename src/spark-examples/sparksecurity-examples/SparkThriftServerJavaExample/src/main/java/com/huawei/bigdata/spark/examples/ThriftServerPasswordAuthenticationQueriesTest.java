package com.huawei.bigdata.spark.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class ThriftServerPasswordAuthenticationQueriesTest {
    private static Properties properties = new Properties();

    private static void init() throws ClassNotFoundException {
        properties.setProperty("user", "YourUserName"); // need to change the value based on the cluster information
        // Hard-coded password or plaintext password in code poses significant security risks. Encrypt and store them in configuration files or environment variables and decrypt them when needed.
        properties.setProperty("password", "password");
    }

    public static void main(String[] args) {
        Connection connection = null;
        ResultSet resultSet = null;
        PreparedStatement statement = null;
        // need to change the value based on the cluster information
        String jdbcUrl = "jdbc:hive2://192.168.42.247:24002,192.168.42.233:24002,192.168.42.224:24002/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=sparkthriftserver;saslQop=auth-conf;auth=KERBEROS;";
        // When hive.server2.use.SSL=true, you need to add the ssl=true parameter to the JDBC URL.
        // jdbcUrl = jdbcUrl + "ssl=true"
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            init();
            String sql = "show tables";
            connection = DriverManager.getConnection(jdbcUrl, properties);
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
