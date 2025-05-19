/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.fusioninsight.hetu.example.springboot.restclient.util;

import com.huawei.fusioninsight.hetu.example.springboot.restclient.service.ConnectionConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

public class HetuDataSourceUtil
{
    private static final Logger logger = LogManager.getLogger(HetuDataSourceUtil.class);
    private static final String JDBC_DRIVER = "io.trino.jdbc.TrinoDriver";
    private static final String DB_URL_PATTERN = "jdbc:trino://%s/%s/%s?serviceDiscoveryMode=hsfabric";

    public static Connection createConnection(ConnectionConfig connectionConfig) throws Exception
    {
        Connection connection = null;
        try {
            Class.forName(JDBC_DRIVER);
            Properties info = new Properties();
            info.put("user", connectionConfig.getUser());
            info.put("SSL", connectionConfig.getSsl());
            info.put("tenant", connectionConfig.getTenant());
            if ("true".equalsIgnoreCase(connectionConfig.getSsl())) {
                info.put("password", connectionConfig.getPassword());
            }
            String dbUrl = String.format(DB_URL_PATTERN, connectionConfig.getHost(), connectionConfig.getCatalog(), connectionConfig.getSchema());

            connection = DriverManager.getConnection(dbUrl, info);
        }
        catch (Exception e) {
            logger.error("Init connection failed.", e);
            throw new Exception(e);
        }
        return connection;
    }

    public static String executeQuery(Connection connection, String sql) throws Exception
    {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            ResultSetMetaData resultMetaData = resultSet.getMetaData();
            int columnCount = resultMetaData.getColumnCount();
            StringBuilder resultMsg = new StringBuilder();
            for (int i = 1; i <= columnCount; i++) {
                resultMsg.append(resultMetaData.getColumnLabel(i)).append("\t");
            }

            StringBuilder result = new StringBuilder();
            String separator = System.getProperty("line.separator");
            while (resultSet.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    result.append(resultSet.getString(i)).append("\t");
                }
                result.append(separator);
            }
            return resultMsg + separator + result;
        }
        catch (Exception e) {
            logger.error("Execute sql {} failed.", sql, e);
            throw new Exception(e);
        }
    }
}
