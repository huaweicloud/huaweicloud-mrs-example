/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.hetu;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * The example code to connect hetu jdbc server and execute sql statement
 *
 * @since 2022-01-17
 */
public class JDBCExampleKeytabFabric {
    private static Properties properties = new Properties();
    private final static String PATH_TO_JAAS_ZK_CONF = Objects.requireNonNull(JDBCExampleKeytabFabric.class.getClassLoader()
                    .getResource("jaas-zk.conf"))
            .getPath();
    private final static String PATH_TO_KRB5_CONF = Objects.requireNonNull(JDBCExampleKeytabFabric.class.getClassLoader()
                    .getResource("krb5.conf"))
            .getPath();
    private final static String PATH_TO_USER_KEYTAB = Objects.requireNonNull(JDBCExampleKeytabFabric.class.getClassLoader()
                    .getResource("user.keytab"))
            .getPath();

    private static void init() throws ClassNotFoundException {
        System.setProperty("user.timezone", "UTC");
        System.setProperty("java.security.auth.login.config", PATH_TO_JAAS_ZK_CONF);
        System.setProperty("java.security.krb5.conf", PATH_TO_KRB5_CONF);
        properties.setProperty("user", "YourUserName"); // need to change the value based on the cluster information
        properties.setProperty("SSL", "true");
        properties.setProperty("tenant", "YourTenant");
        properties.setProperty("KerberosConfigPath", PATH_TO_KRB5_CONF);
        properties.setProperty("KerberosPrincipal", "YourUserName"); // need to change the value based on the cluster information
        properties.setProperty("KerberosKeytabPath", PATH_TO_USER_KEYTAB);
        properties.setProperty("KerberosRemoteServiceName", "HTTP");
        properties.setProperty("tenant", "YourTenant"); // need to change the value based on the cluster information
        properties.setProperty("deploymentMode", "on_yarn");
        properties.setProperty("ZooKeeperAuthType", "kerberos");
        properties.setProperty("ZooKeeperSaslClientConfig", "Client");
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
        String url = "jdbc:trino://192.168.1.130:29902,192.168.1.131:29902,192.168.1.132:29902/hive/default?serviceDiscoveryMode=hsfabric";

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
