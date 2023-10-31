/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2023. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.huawei.hadoop.security.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * Phoenix development description sample code,
 * which describes how to use Phoenix APIs to
 * access HBase clusters and perform SQL operations.
 *
 * @since 2020-09-21
 */

public class PhoenixSample {
    private static final Logger LOG = LoggerFactory.getLogger(PhoenixSample.class.getName());
    Configuration conf = null;
    java.util.Properties props = new java.util.Properties();

    /**
     * Constructor using default client configuration
     *
     * @throws IOException When the creation fails, an exception is thrown.
     */
    public PhoenixSample() throws SQLException {
        this(Utils.createClientConf());
    }

    /**
     * Constructor with a client configuration
     *
     * @param conf Client configuration
     * @throws SQLException When the creation fails, an exception is thrown.
     */
    public PhoenixSample(Configuration conf) throws SQLException {
        DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        this.conf = conf;
        Iterator<Entry<String, String>> it = conf.iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            props.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * PhoenixSample test
     */
    public void test() {
        testCreateTable();
        testPut();
        testSelect();
        testDrop();
    }

    /**
     * Create Table
     */
    public void testCreateTable() {
        LOG.info("Entering testCreateTable.");
        String url = "jdbc:phoenix:" + conf.get("hbase.zookeeper.quorum");
        // Create table
        String createTableSQL =
            "CREATE TABLE IF NOT EXISTS TEST (id integer not null primary key, name varchar, "
                + "account char(6), birth date)";
        try (Connection conn = DriverManager.getConnection(url, props);
            Statement stat = conn.createStatement()) {
            // Execute Create SQL
            stat.executeUpdate(createTableSQL);
            LOG.info("Create table successfully.");
        } catch (SQLException e) {
            LOG.error("Create table failed.", e);
        }
        LOG.info("Exiting testCreateTable.");
    }

    /**
     * Put data
     */
    public void testPut() {
        LOG.info("Entering testPut.");
        String url = "jdbc:phoenix:" + conf.get("hbase.zookeeper.quorum");
        // Insert
        String upsertSQL =
            "UPSERT INTO TEST VALUES(1,'John','100000', TO_DATE('1980-01-01','yyyy-MM-dd'))";
        try (Connection conn = DriverManager.getConnection(url, props);
            Statement stat = conn.createStatement()) {
            // Execute Update SQL
            stat.executeUpdate(upsertSQL);
            conn.commit();
            LOG.info("Put successfully.");
        } catch (SQLException e) {
            LOG.error("Put failed.", e);
        }
        LOG.info("Exiting testPut.");
    }

    /**
     * Select Data
     */
    public void testSelect() {
        LOG.info("Entering testSelect.");
        String url = "jdbc:phoenix:" + conf.get("hbase.zookeeper.quorum");
        // Query
        String querySQL = "SELECT * FROM TEST WHERE id = ?";
        Connection conn = null;
        PreparedStatement preStat = null;
        Statement stat = null;
        ResultSet result = null;
        try {
            // Create Connection
            conn = DriverManager.getConnection(url, props);
            // Create Statement
            stat = conn.createStatement();
            // Create PrepareStatement
            preStat = conn.prepareStatement(querySQL);
            // Execute query
            preStat.setInt(1, 1);
            result = preStat.executeQuery();
            // Get result
            while (result.next()) {
                int id = result.getInt("id");
                String name = result.getString(1);
                LOG.info("id: {} name: {}", id, name);
            }
            LOG.info("Select successfully.");
        } catch (SQLException e) {
            LOG.error("Select failed.", e);
        } finally {
            if (null != result) {
                try {
                    result.close();
                } catch (SQLException e2) {
                    LOG.error("Result close failed.", e2);
                }
            }
            if (null != stat) {
                try {
                    stat.close();
                } catch (SQLException e2) {
                    LOG.error("Stat close failed.", e2);
                }
            }
            if (null != conn) {
                try {
                    conn.close();
                } catch (SQLException e2) {
                    LOG.error("Connection close failed.", e2);
                }
            }
        }
        LOG.info("Exiting testSelect.");
    }

    /**
     * Drop Table
     */
    public void testDrop() {
        LOG.info("Entering testDrop.");
        String url = "jdbc:phoenix:" + conf.get("hbase.zookeeper.quorum");
        // Delete table
        String dropTableSQL = "DROP TABLE TEST";

        try (Connection conn = DriverManager.getConnection(url, props);
            Statement stat = conn.createStatement()) {
            stat.executeUpdate(dropTableSQL);
            LOG.info("Drop successfully.");
        } catch (SQLException e) {
            LOG.error("Drop failed.", e);
        }
        LOG.info("Exiting testDrop.");
    }
}

