/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.Connection;

/**
 * doirs springboot sample example service
 *
 * @since 2024-02-22
 */
@Service
public class DBalancerExampleService {
    private static final Logger logger = LogManager.getLogger(DBalancerExampleService.class);

    public String executeSql() {
        // Don't add a semicolon at the end.
        String dbName = "demo_db";
        String tableName = "test_tbl";
        String createDatabaseSql = "create database if not exists demo_db";
        String createTableSql = "create table if not exists " + dbName + "." + tableName +  " (\n" +
                "c1 int not null,\n" +
                "c2 int not null,\n" +
                "c3 string not null\n" +
                ") engine=olap\n" +
                "unique key(c1, c2)\n" +
                "distributed by hash(c1) buckets 1";
        String insertTableSql = "insert into " + dbName + "." + tableName + " values(?, ?, ?)";
        String querySql = "select * from " + dbName + "." + tableName + " limit 10";
        String dropSql = "drop table " + dbName + "." + tableName;
        String dropDb = "drop database " + dbName;

        StringBuilder result = new StringBuilder();
        String separator = System.getProperty("line.separator");
        result.append("=========================== Doris Example Start ===========================");

        try (Connection connection = DBalancerDataSourceUtil.createConnection()) {
            // Create database
            result.append(separator).append("Start create database.");
            DBalancerDataSourceUtil.execDDL(connection, createDatabaseSql);
            result.append(separator).append("Database created successfully.");

            // Create table
            result.append(separator).append("Start create table.");
            DBalancerDataSourceUtil.execDDL(connection, createTableSql);
            result.append(separator).append("Table created successfully.");

            // Insert into data to table
            result.append(separator).append("Start to insert data into the table.");
            DBalancerDataSourceUtil.insert(connection, insertTableSql);
            result.append(separator).append("Inserting data to the table succeeded.");

            // Query table data
            result.append(separator).append("Start to query table data.");
            String queryResult = DBalancerDataSourceUtil.executeQuery(connection, querySql);
            result.append(separator).append("Query result : ").append(separator).append(queryResult);
            result.append(separator).append("Querying table data succeeded.");

            // Delete table
            result.append(separator).append("Start to delete the table.");
            DBalancerDataSourceUtil.execDDL(connection, dropSql);
            result.append(separator).append("Table deleted successfully.");

            // Delete database
            result.append(separator).append("Start to delete the database.");
            DBalancerDataSourceUtil.execDDL(connection, dropDb);
            result.append(separator).append("Database deleted successfully.");
            result.append(separator).append("=========================== Doris Example End ===========================");
        } catch (Exception e) {
            logger.error("Doris springboot example execution failure, detail exception : " + e);
            return "Doris springboot example execution failure, detail exception : " + e;
        }

        logger.info("Doris springboot example execution successfully.");
        return result.toString();
    }
}
