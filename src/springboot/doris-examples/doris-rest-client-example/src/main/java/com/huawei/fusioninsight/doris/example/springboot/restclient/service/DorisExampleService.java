/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.doris.example.springboot.restclient.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.*;

/**
 * doirs springboot样例service
 *
 * @since 2022-11-14
 */
@Service
public class DorisExampleService {
    private static final Logger logger = LogManager.getLogger(DorisExampleService.class);

    public String executeSql() {
        // 注意末尾不要加 分号 ";"
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

        StringBuilder result = new StringBuilder();
        String separator = System.getProperty("line.separator");
        result.append("=========================== Doris Example Start ===========================");

        try (Connection connection = DorisDataSourceUtil.createConnection()) {
            // 创建数据库
            result.append(separator).append("Start create database.");
            DorisDataSourceUtil.execDDL(connection, createDatabaseSql);
            result.append(separator).append("Database created successfully.");

            // 创建表
            result.append(separator).append("Start create table.");
            DorisDataSourceUtil.execDDL(connection, createTableSql);
            result.append(separator).append("Table created successfully.");

            // 插入表数据
            result.append(separator).append("Start to insert data into the table.");
            DorisDataSourceUtil.insert(connection, insertTableSql);
            result.append(separator).append("Inserting data to the table succeeded.");

            // 查询表数据
            result.append(separator).append("Start to query table data.");
            String queryResult = DorisDataSourceUtil.executeQuery(connection, querySql);
            result.append(separator).append("Query result : ").append(separator).append(queryResult);
            result.append(separator).append("Querying table data succeeded.");

            // 删除表
            result.append(separator).append("Start to delete the table.");
            DorisDataSourceUtil.execDDL(connection, dropSql);
            result.append(separator).append("Table deleted successfully.");
            result.append(separator).append("=========================== Doris Example End ===========================");
        } catch (Exception e) {
            logger.error("Doris springboot example execution failure, detail exception : " + e);
            return "Doris springboot example execution failure, detail exception : " + e;
        }

        logger.info("Doris springboot example execution successfully.");
        return result.toString();
    }
}
