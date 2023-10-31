/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.clickhouse.examples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 功能描述：
 * 本样例代码主要用于帮助客户如何使用事务新特性。
 * @since 2021-03-30
 */
public class Demo {
    private final static Logger log = LogManager.getLogger(Demo.class);

    private String loadBalancerIPList;
    private String loadBalancerHttpPort;
    static String user;
    private static String clusterName;
    static Boolean isSec;
    static String password;
    static List<String> ckLbServerList;
    private static String tableName;
    private static String databaseName;
    private Util util = new Util();
    private static int batchRows;
    private static int batchNum;
    static Boolean sslUsed;
    static Boolean isMachineUser;
    static Boolean useTransaction;
    static Boolean autoCommit;

    public static void main(String[] args) {
        Demo demo = new Demo();
        try {
            demo.getProperties();
            demo.getCkLbServerList();
            demo.dropTable(databaseName, tableName, clusterName);
            demo.createDatabase(databaseName, clusterName);
            demo.createTable(databaseName, tableName, clusterName);
            demo.insertData(databaseName, tableName, batchNum, batchRows);
            demo.queryData(databaseName, tableName);

        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    private void getProperties() throws Exception {
        Properties properties = new Properties();
        String proPath =
                System.getProperty("user.dir")
                        + File.separator
                        + "conf"
                        + File.separator
                        + "clickhouse-example.properties";
        try {
            properties.load(new FileInputStream(new File(proPath)));
        } catch (IOException e) {
            log.error("Failed to load properties file.");
            throw e;
        }
        loadBalancerIPList = properties.getProperty("loadBalancerIPList");
        sslUsed = Boolean.parseBoolean(properties.getProperty("sslUsed"));
        isMachineUser = Boolean.parseBoolean(properties.getProperty("isMachineUser"));
        useTransaction = Boolean.parseBoolean(properties.getProperty("useTransaction"));
        autoCommit = Boolean.parseBoolean(properties.getProperty("autoCommit"));
        if (sslUsed) {
            loadBalancerHttpPort = properties.getProperty("loadBalancerHttpsPort");
        } else {
            loadBalancerHttpPort = properties.getProperty("loadBalancerHttpPort");
        }
        isSec = Boolean.parseBoolean(properties.getProperty("CLICKHOUSE_SECURITY_ENABLED"));
        if (isSec) {
            password = properties.getProperty("password");
        }
        user = properties.getProperty("user");
        clusterName = properties.getProperty("clusterName");
        databaseName = properties.getProperty("databaseName");
        tableName = properties.getProperty("tableName");
        batchRows = Integer.parseInt(properties.getProperty("batchRows"));
        batchNum = Integer.parseInt(properties.getProperty("batchNum"));
    }

    private void getCkLbServerList() {
        if (null == loadBalancerIPList || loadBalancerIPList.length() == 0) {
            log.error("clickhouseBalancer ip list is empty.");
            return;
        }
        ckLbServerList = Arrays.asList(loadBalancerIPList.split(","));
        for (int i = 0; i < ckLbServerList.size(); i++) {
            String tmpIp = ckLbServerList.get(i);
            if (tmpIp.contains(":")) {
                tmpIp = "[" + tmpIp + "]";
                ckLbServerList.set(i, tmpIp);
            }
            String tmpServer = ckLbServerList.get(i) + ":" + loadBalancerHttpPort;
            ckLbServerList.set(i, tmpServer);
            log.info("ckLbServerList current member is {}, ClickhouseBalancer is {}", i, ckLbServerList.get(i));
        }
    }

    private void dropTable(String databaseName, String tableName, String clusterName) throws Exception {
        String dropLocalTableSql =
                "drop table if exists " + databaseName + "." + tableName + " on cluster " + clusterName + " no delay";
        String dropDisTableSql =
                "drop table if exists "
                        + databaseName
                        + "."
                        + tableName
                        + "_all"
                        + " on cluster "
                        + clusterName
                        + " no delay";
        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add(dropLocalTableSql);
        sqlList.add(dropDisTableSql);
        util.exeSql(sqlList);
    }

    private void createDatabase(String databaseName, String clusterName) throws Exception {
        String createDbSql = "create database if not exists " + databaseName + " on cluster " + clusterName;
        util.exeSql(createDbSql);
    }

    private void createTable(String databaseName, String tableName, String clusterName) throws Exception {
        String createSql =
                "create table "
                        + databaseName
                        + "."
                        + tableName
                        + " on cluster "
                        + clusterName
                        + " (name String, age UInt8, date Date)engine=ReplicatedMergeTree('/clickhouse/tables/{shard}/"
                        + databaseName
                        + "."
                        + tableName
                        + "',"
                        + "'{replica}') partition by toYYYYMM(date) order by age";
        String createDisSql =
                "create table "
                        + databaseName
                        + "."
                        + tableName
                        + "_all"
                        + " on cluster "
                        + clusterName
                        + " as "
                        + databaseName
                        + "."
                        + tableName
                        + " ENGINE = Distributed(default_cluster,"
                        + databaseName
                        + ","
                        + tableName
                        + ", rand());";
        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add(createSql);
        sqlList.add(createDisSql);
        util.exeSql(sqlList);
    }

    private void insertData(String databaseName, String tableName, int batchNum, int batchRows) throws Exception {
        util.insertData(databaseName, tableName, batchNum, batchRows);
    }

    private void queryData(String databaseName, String tableName) throws Exception {
        String querySql1 = "select * from " + databaseName + "." + tableName + "_all" + " order by age limit 10";
        String querySql2 =
                "select toYYYYMM(date),count(1) from "
                        + databaseName
                        + "."
                        + tableName
                        + "_all"
                        + " group by toYYYYMM(date) order by count(1) DESC limit 10";
        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add(querySql1);
        sqlList.add(querySql2);
        ArrayList<ArrayList<ArrayList<String>>> result = util.exeSql(sqlList);
        for (ArrayList<ArrayList<String>> singleResult : result) {
            for (ArrayList<String> strings : singleResult) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String string : strings) {
                    stringBuilder.append(string).append("\t");
                }
                log.info(stringBuilder.toString());
            }
        }
    }
}
