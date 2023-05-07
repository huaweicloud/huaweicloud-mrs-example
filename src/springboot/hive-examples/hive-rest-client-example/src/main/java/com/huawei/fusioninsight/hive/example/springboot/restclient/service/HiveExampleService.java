/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.hive.example.springboot.restclient.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;

import java.sql.*;

/**
 * elasticsearch springboot样例service
 *
 * @since 2022-11-14
 */
@Service
public class HiveExampleService {
    private static final Logger LOG = LogManager.getLogger(HiveExampleService.class);

    /**
     * 定义Hive样例使用的SQL
     */
    private static final String[] SQLS = {
        "CREATE TABLE IF NOT EXISTS employees_infoa(id INT, age INT, name STRING)",
        "INSERT INTO employees_infoa VALUES(001, 31, 'SJK'),(002, 25, 'HS'),(003, 28, 'HT')",
        "SELECT * FROM employees_infoa",
        "DROP TABLE employees_infoa"
    };

    /**
     * 执行hive sql语句
     */
    public String executesql() {
        Connection connection = null;
        StringBuilder result = new StringBuilder();
        String separator = System.getProperty("line.separator");
        try {
            connection = HiveDataSourceUtil.createConnection();

            result.append("=========================== Hive Example Start ===========================");
            result.append(separator).append("Start create table.");
            HiveDataSourceUtil.executeSql(connection, SQLS[0]);
            result.append(separator).append("Table created successfully.");

            result.append(separator).append("Start to insert data into the table.");
            HiveDataSourceUtil.executeSql(connection, SQLS[1]);
            result.append(separator).append("Inserting data to the table succeeded.");

            result.append(separator).append("Start to query table data.");
            String queryResult = HiveDataSourceUtil.executeQuery(connection, SQLS[2]);
            result.append(separator).append("Query result : ").append(separator).append(queryResult);
            result.append(separator).append("Querying table data succeeded.");

            result.append(separator).append("Start to delete the table.");
            HiveDataSourceUtil.executeSql(connection, SQLS[3]);
            result.append(separator).append("Table deleted successfully.");
            result.append(separator).append("=========================== Hive Example End ===========================");

        } catch (Exception e) {
            LOG.error("Hive springboot example execution failure, detail exception : " + e);
            return "Hive springboot example execution failure, detail exception : " + e;
        } finally {
            HiveDataSourceUtil.closeConnection(connection);
        }
        LOG.info("Hive springboot example execution successfully.");
        return result.toString();
    }
}
