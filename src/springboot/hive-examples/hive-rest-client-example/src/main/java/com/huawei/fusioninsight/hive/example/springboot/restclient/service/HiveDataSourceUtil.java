/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.hive.example.springboot.restclient.service;

import com.huawei.fusioninsight.hive.example.springboot.restclient.security.KerberosUtil;
import com.huawei.fusioninsight.hive.example.springboot.restclient.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class HiveDataSourceUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HiveDataSourceUtil.class);
    private static final String HIVE_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    private static String ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = null;
    private static String FOUR_EMPTY = "    ";
    private static String KRB5_FILE = null;
    private static String USER_NAME = null;
    private static String USER_KEYTAB_FILE = null;
    private static Configuration CONF = null;

    /**
     * zookeeper节点ip和端口列表
     */
    private static String zkQuorum = null;
    private static String auth = null;
    private static String sasl_qop = null;
    private static String zooKeeperNamespace = null;
    private static String serviceDiscoveryMode = null;
    private static String principal = null;
    private static String auditAddition = null;
    private static String AUTH_HOST_NAME = null;

    /**
     * Get user realm process
     */
    public static String getUserRealm() {
        String serverRealm = System.getProperty("SERVER_REALM");
        if (serverRealm != null && serverRealm != "") {
            AUTH_HOST_NAME = "hadoop." + serverRealm.toLowerCase();
        } else {
            serverRealm = KerberosUtil.getKrb5DomainRealm();
            if (serverRealm != null && serverRealm != "") {
                AUTH_HOST_NAME = "hadoop." + serverRealm.toLowerCase();
            } else {
                AUTH_HOST_NAME = "hadoop";
            }
        }
        return AUTH_HOST_NAME;
    }

    private static void init() throws IOException {
        CONF = new Configuration();
        Properties clientInfo;
        String userdir =
            System.getProperty("user.dir")
                + File.separator
                + "src"
                + File.separator
                + "main"
                + File.separator
                + "resources"
                + File.separator;
        InputStream fileInputStream = null;
        /**
         * "hiveclient.properties"为客户端配置文件，如果使用多实例特性，需要把该文件换成对应实例客户端下的"hiveclient.properties"
         * "hiveclient.properties"文件位置在对应实例客户端安裝包解压目录下的config目录下
         */
        String hiveclientProp = userdir + "hiveclient.properties";
        try {
            clientInfo = new Properties();
            File propertiesFile = new File(hiveclientProp);
            fileInputStream = new FileInputStream(propertiesFile);
            clientInfo.load(fileInputStream);
        } catch (IOException e) {
            LOG.error("Failed to load hive client configuration from {}", hiveclientProp);
            throw new IOException("Failed to load hive client configuration from " + hiveclientProp, e);
        } finally {
            if (fileInputStream != null) {
                fileInputStream.close();
            }
        }

        /*
         * zkQuorum获取后的格式为"xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002,xxx.xxx.xxx.xxx:24002";
         * "xxx.xxx.xxx.xxx"为集群中ZooKeeper所在节点的业务IP，端口默认是24002
         */
        zkQuorum = clientInfo.getProperty("zk.quorum");
        auth = clientInfo.getProperty("auth");
        sasl_qop = clientInfo.getProperty("sasl.qop");
        zooKeeperNamespace = clientInfo.getProperty("zooKeeperNamespace");
        serviceDiscoveryMode = clientInfo.getProperty("serviceDiscoveryMode");
        principal = clientInfo.getProperty("principal");
        auditAddition = clientInfo.getProperty("auditAddition");
        KRB5_FILE = userdir + "krb5.conf";
        System.setProperty("java.security.krb5.conf", KRB5_FILE);
        // 设置新建用户的USER_NAME，其中"xxx"指代之前创建的用户名，例如创建的用户为user，则USER_NAME为user
        USER_NAME = clientInfo.getProperty("user.name");

        if ("KERBEROS".equalsIgnoreCase(auth)) {
            // 设置客户端的keytab和zookeeper认证principal
            USER_KEYTAB_FILE = "src/main/resources/user.keytab";
            ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper/" + getUserRealm();
            System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL);
            LoginUtil.login(USER_NAME, USER_KEYTAB_FILE, KRB5_FILE, CONF);
        }

        //zookeeper开启ssl时需要设置JVM参数
        LoginUtil.processZkSsl(clientInfo);
    }

    public static Connection createConnection() {
        // 参数初始化
        try {
            init();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // 拼接JDBC URL
        StringBuilder strBuilder = new StringBuilder("jdbc:hive2://").append(zkQuorum).append("/");

        if ("KERBEROS".equalsIgnoreCase(auth)) {
            strBuilder
                .append(";serviceDiscoveryMode=")
                .append(serviceDiscoveryMode)
                .append(";zooKeeperNamespace=")
                .append(zooKeeperNamespace)
                .append(";sasl.qop=")
                .append(sasl_qop)
                .append(";auth=")
                .append(auth)
                .append(";principal=")
                .append(principal)
                .append(";user.principal=")
                .append(USER_NAME)
                .append(";user.keytab=")
                .append(USER_KEYTAB_FILE);
        } else {
            /* 普通模式 */
            strBuilder
                .append(";serviceDiscoveryMode=")
                .append(serviceDiscoveryMode)
                .append(";zooKeeperNamespace=")
                .append(zooKeeperNamespace)
                .append(";auth=none");
        }
        if (auditAddition != null && !auditAddition.isEmpty()) {
            strBuilder.append(";auditAddition=").append(auditAddition);
        }
        String url = strBuilder.toString();

        Connection connection = null;
        try {
            // 加载Hive JDBC驱动
            Class.forName(HIVE_DRIVER);
            // 获取JDBC连接
            // 如果使用的是普通模式，那么第二个参数需要填写正确的用户名，否则会以匿名用户(anonymous)登录
            return DriverManager.getConnection(url, "", "");
        } catch (ClassNotFoundException | SQLException e) {
            LOG.error("Create connection failed.");
            throw new RuntimeException("Create connection failed.", e);
        }

    }

    /**
     * 关闭JDBC连接
     *
     * @param conn JDBC连接
     */
    public static void closeConnection(Connection conn) {
        try {
            if (null != conn) {
                conn.close();
            }
        } catch (SQLException e) {
            LOG.error("Faild to close connection.");
            throw new RuntimeException("Faild to close connection.", e);
        }
    }

    public static void executeSql(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(sql);
            statement.execute();
        } finally {
            try {
                if (null != statement) {
                    statement.close();
                }
            } catch (SQLException e) {
                LOG.error("Failed to close PreparedStatement, exception : " + e);
            }
        }
    }

    public static String executeQuery(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        ResultSetMetaData resultMetaData = null;
        try {
            // 执行HQL
            statement = connection.prepareStatement(sql);
            resultSet = statement.executeQuery();

            // 输出查询的列名到控制台
            resultMetaData = resultSet.getMetaData();
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
        } finally {
            try {
                if (null != resultSet) {
                    resultSet.close();
                }

                if (null != statement) {
                    statement.close();
                }
            } catch (SQLException e) {
                LOG.error("Failed to close ResultSet or PreparedStatement, exception : " + e);
            }
        }
    }

}
