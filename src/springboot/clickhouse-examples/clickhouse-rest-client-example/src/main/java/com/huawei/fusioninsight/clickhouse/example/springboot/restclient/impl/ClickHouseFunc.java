/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.clickhouse.example.springboot.restclient.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * ClickHouse连接数据库执行query类
 *
 */
public class ClickHouseFunc {
    private static final Logger log = LogManager.getLogger(ClickHouseFunc.class);

    private String loadBalancerIPList;
    private String loadBalancerHttpPort;
    static String user;
    private static String clusterName;
    static Boolean isSec;
    static String password;
    static List<String> ckLbServerList;
    private static String tableName;
    private static String databaseName;
    private static int batchRows;
    private static int batchNum;
    static Boolean sslUsed;
    static Boolean isMachineUser;

    /**
     * 执行删除表、创建数据库、创建表、插入数据、查询数据一系列的query
     *
     * @since 2022-12-16
     */
    public String executeQuery() {
        ClickHouseFunc clickHouseFunc = new ClickHouseFunc();
        try {
            clickHouseFunc.getProperties();
            log.info("loadBalancerIPList is {}, loadBalancerHttpPort is {}, user is {}, clusterName is {}, isSec is {}.",
                    clickHouseFunc.loadBalancerIPList, clickHouseFunc.loadBalancerHttpPort, user, clusterName, isSec);
            clickHouseFunc.getCkLbServerList();
            clickHouseFunc.dropTable(databaseName, tableName, clusterName);
            clickHouseFunc.createDatabase(databaseName, clusterName);
            clickHouseFunc.createTable(databaseName, tableName, clusterName);
            clickHouseFunc.insertData(databaseName, tableName, batchNum, batchRows);
            clickHouseFunc.queryData(databaseName, tableName);
            return "ClickHouse springboot client runs normally.";
        } catch (Exception e) {
            log.error(e.toString());
            return "Some exception happened, please check the log.";
        }
    }
    
    private void getProperties() throws Exception {
        Properties properties = new Properties();
        String proPath = System.getProperty("user.dir") + File.separator + "conf"
                + File.separator + "clickhouse-example.properties";
        try {
            properties.load(new FileInputStream(new File(proPath)));
        } catch (IOException e) {
            log.error("Failed to load properties file.");
            throw e;
        }
        loadBalancerIPList = properties.getProperty("loadBalancerIPList");
        sslUsed = Boolean.parseBoolean(properties.getProperty("sslUsed"));
        isMachineUser = Boolean.parseBoolean(properties.getProperty("isMachineUser"));
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
        String dropLocalTableSql = "drop table if exists " + databaseName + "." + tableName + " on cluster " + clusterName + " no delay";
        String dropDisTableSql = "drop table if exists " + databaseName + "." + tableName + "_all" + " on cluster " + clusterName + " no delay";
        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add(dropLocalTableSql);
        sqlList.add(dropDisTableSql);
        Util.executeQueryList(sqlList);
    }

    private void createDatabase(String databaseName, String clusterName) throws Exception  {
        String createDbSql = "create database if not exists " + databaseName + " on cluster " + clusterName;
        Util.executeQuery(createDbSql);
    }

    private void createTable(String databaseName, String tableName, String clusterName) throws Exception {
        String createSql = "create table " + databaseName + "." + tableName + " on cluster " + clusterName
                + " (name String, age UInt8, date Date)engine=ReplicatedMergeTree('/clickhouse/tables/{shard}/" + databaseName
                + "." + tableName + "'," + "'{replica}') partition by toYYYYMM(date) order by age";
        String createDisSql = "create table " + databaseName + "." + tableName + "_all" + " on cluster " + clusterName + " as "
                + databaseName + "." + tableName + " ENGINE = Distributed(default_cluster," + databaseName + "," + tableName + ", rand());";
        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add(createSql);
        sqlList.add(createDisSql);
        Util.executeQueryList(sqlList);
    }

    private void insertData(String databaseName, String tableName, int batchNum, int batchRows) throws Exception {
        Util.insertData(databaseName, tableName, batchNum, batchRows);
    }

    private void queryData(String databaseName, String tableName) throws Exception {
        String querySql1 = "select * from " + databaseName + "." + tableName + "_all" + " order by age limit 10";
        String querySql2 = "select toYYYYMM(date),count(1) from " + databaseName + "." + tableName + "_all"
                + " group by toYYYYMM(date) order by count(1) DESC limit 10";
        ArrayList<String> sqlList = new ArrayList<String>();
        sqlList.add(querySql1);
        sqlList.add(querySql2);
        List<List<List<String>>> result = Util.executeQueryList(sqlList);
        for (List<List<String>> singleResult : result) {
            for (List<String> strings : singleResult) {
                StringBuilder stringBuilder = new StringBuilder();
                for (String string : strings) {
                    stringBuilder.append(string).append("\t");
                }
                log.info(stringBuilder.toString());
            }
        }
    }
}
