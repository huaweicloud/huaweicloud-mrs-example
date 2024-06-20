/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.bigdata.iceberg.examples;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 功能描述：
 *   iceberg 建表，插入，更新，删除；
 *   创建分支，分支写入，分支查询；
 *   元数据查询
 */
public class IcebergSqlExample {
    private SparkSession spark;
    private String catalogName = "local";
    private String dataBaseName = "iceberg_db";
    private String tableName = "tb1";
    private String branchName = "audit";

    private String fullTableName = catalogName + "." + dataBaseName + "." + tableName;

    public static void main(String[] args) {
        IcebergSqlExample example = new IcebergSqlExample();
        try {
            // table operate
            example.initSpark();
            example.createTable();
            example.insert();
            example.updateTable();
            example.queryTable();

            // branch feature
            example.createBranch();
            example.insertToBranch();
            example.queryBranch();

            // Meta data table info
            example.showTableHistory();
            example.showTableRef();

            // drop table
            example.dropTable();
        } finally {
            example.closeSpark();
        }
    }

    public void dropTable() {
        String sql = String.format("drop table %s", fullTableName);
        spark.sql(sql);
    }

    public void showTableHistory() {
        String sql = String.format("select * from %s.history", fullTableName);
        spark.sql(sql).show(false);
    }

    public void showTableRef() {
        String sql = String.format("select * from %s.refs", fullTableName);
        spark.sql(sql).show(false);
    }

    public void initSpark() {
        spark = SparkSessionFactory.buildSparkSession(catalogName);
    }

    public void createBranch() {
        String sql = String.format("ALTER TABLE %s CREATE BRANCH IF NOT EXISTS %s RETAIN 30 DAYS", fullTableName, branchName);
        spark.sql(sql);
    }

    public void insertToBranch() {
        // insert to 'audit' branch begin with 'branch_'
        String sql = String.format("INSERT INTO %s.branch_%s VALUES (4, 'e'), (5, 'f')", fullTableName, branchName);
        spark.sql(sql);
    }

    public void queryBranch() {
        String sql = String.format("select * from %s VERSION AS OF '%s'", fullTableName, branchName);
        Dataset<Row> result = spark.sql(sql);
        result.show();
    }

    public void createTable() {
        spark.sql(String.format("create database if not exists %s", dataBaseName));

        String sql = String.format("CREATE TABLE if not exists %s (id bigint, data string) USING iceberg TBLPROPERTIES ('write.format.default'='orc')", fullTableName);
        spark.sql(sql);
    }

    public void insert() {
        spark.sql(String.format("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", fullTableName));
        Dataset<Row> result = spark.sql(String.format("select * from %s", fullTableName));
        result.show();
    }

    public void updateTable() {
        spark.sql(String.format("UPDATE %s set data = 'AAA' where id = 1", fullTableName));
        Dataset<Row> result = spark.sql(String.format("select * from %s where id = 1", fullTableName));
        result.show();
    }

    public void queryTable() {
        Dataset<Row> result = spark.sql(String.format("select * from %s where id > 1", fullTableName));
        result.show();
    }

    public void closeSpark() {
        if (spark == null) {
            return;
        }

        spark.close();
    }

}
