/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.huawei.hadoop.security.Utils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This sample used to test multi-thread read/write hbase table with doing preload cache.
 * Background:
 * When we set a small value for "hbase.rpc.timeout" or "hbase.client.operation.timeout" in hbase client,
 * do these steps will cause timeout:
 * 1.Restart Application(HBase client), will create a new connection to hbase, and region location cache will be empty.
 * 2.Do multi-thread hbase operations, each thread will cache region location, but only one thread could query
 * hbase:meta to cache region location(This is a hbase backpressure feature to prevent meta table be a hotspot),
 * cache region location may cost 100-3000ms(depends on region numbers need cache), after this thread cached regions,
 * other thread may time out when they cache region locations.
 * Solution:
 * Cache all region locations for a table before multi-thread hbase operations.
 */
public class MultiThreadSample {
    private static final Logger LOG = LoggerFactory.getLogger(MultiThreadSample.class);

    private final Connection conn;

    private final TableName tableName = TableName.valueOf("user_table_multi_thread");

    private static final int REGION_NUMS = 300;

    private static final int OPERATIONS_COUNT = 500;

    private static final int SHUT_DOWN_POOL_WAIT_TIMEOUT_SEC = 300;

    public MultiThreadSample() throws IOException {
        Configuration gsiClientConf = HBaseConfiguration.create(Utils.createClientConf());
        Utils.handleZkSslEnabled(gsiClientConf);
        this.conn = ConnectionFactory.createConnection(gsiClientConf);
    }

    public void test() {
        try {
            testMultiThreadReadWrite();
        } finally {
            IOUtils.closeQuietly(conn, e -> LOG.error("Failed to close the connection ", e));
        }
    }

    public void testMultiThreadReadWrite() {
        LOG.info("Entering testMultiThreadReadWrite.");
        createUserTable();
        // Cache all region location of a table.
        preLoadTableRegionLocations();
        // Execute multi-thread hbase operations.
        doOperations();
        dropUserTable();
        LOG.info("Exiting testMultiThreadReadWrite.");
    }

    /**
     * create a multi-region table for test.
     */
    private void createUserTable() {
        // Use number id as first part of rowKey and pre-split 300 regions by decimal string split algorithm.
        ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes("f"))
                .setCompressionType(Compression.Algorithm.SNAPPY)
                .build();
        TableDescriptor td = TableDescriptorBuilder
                .newBuilder(tableName)
                .setColumnFamily(cf)
                .build();
        try (Admin admin = conn.getAdmin()) {
            if (!admin.tableExists(tableName)) {
                RegionSplitter.SplitAlgorithm splitAlgo = RegionSplitter
                        .newSplitAlgoInstance(this.conn.getConfiguration(),
                                RegionSplitter.DecimalStringSplit.class.getName());
                admin.createTable(td, splitAlgo.split(REGION_NUMS));
                LOG.info("Create table success.");
            } else {
                LOG.warn("Table already exists.");
            }
        } catch (IOException e) {
            LOG.error("Create table failed!", e);
        }
    }

    /**
     * Load all region locations of a table.
     */
    private void preLoadTableRegionLocations() {
        try (RegionLocator rl = conn.getRegionLocator(tableName)) {
            long startTime = System.currentTimeMillis();
            rl.getAllRegionLocations();
            LOG.info("Cache table region location success, cost {} ms.", System.currentTimeMillis() - startTime);
        } catch (IOException e) {
            LOG.error("Cache table region location failed!", e);
        }
    }

    /**
     * Execute hbase operations.
     */
    private void doOperations() {
        int cores = Math.max(32, Runtime.getRuntime().availableProcessors());
        // Pool for user service including read/write hbase data.
        ThreadPoolExecutor pool = new ThreadPoolExecutor(cores, cores,
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(cores * 100),
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setUncaughtExceptionHandler((t, e) ->
                                LOG.warn("Thread:{} exited with Exception:{}", t, StringUtils.stringifyException(e)))
                        .setNameFormat("Application-Pool-%d").build());

        for (int i = 0; i < OPERATIONS_COUNT; i++) {
            pool.execute(() -> {
                try (Table table = conn.getTable(tableName)) {
                    Put put = new Put(Bytes.toBytes(
                            ThreadLocalRandom.current().nextLong(0, 100000L) + "#"
                                    + RandomStringUtils.randomAlphabetic(5)));
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("name"),
                            Bytes.toBytes(RandomStringUtils.randomAlphabetic(10)));
                    put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("ts"),
                            Bytes.toBytes(Long.toString(System.currentTimeMillis())));
                    table.put(put);
                } catch (IOException e) {
                    LOG.error("Put data failed ", e);
                }
            });
            pool.execute(() -> {
                Scan scan = new Scan()
                        .withStartRow(Bytes.toBytes(
                                ThreadLocalRandom.current().nextLong(0, 100000L) + "#"))
                        .setLimit(1);
                try (Table table = conn.getTable(tableName);
                     ResultScanner scanner = table.getScanner(scan)) {
                    Result result;
                    while ((result = scanner.next()) != null) {
                        for (Cell cell : result.rawCells()) {
                            LOG.info("ROW:{},CF:{},CQ:{},VALUE:{}",
                                    Bytes.toString(CellUtil.cloneRow(cell)),
                                    Bytes.toString(CellUtil.cloneFamily(cell)),
                                    Bytes.toString(CellUtil.cloneQualifier(cell)),
                                    Bytes.toString(CellUtil.cloneValue(cell)));
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Scan data failed ", e);
                }
            });
        }
        shutDownPool(pool);
        LOG.info("Finish do operations.");
    }

    private void shutDownPool(ThreadPoolExecutor pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(SHUT_DOWN_POOL_WAIT_TIMEOUT_SEC, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
        }
    }

    private void dropUserTable() {
        try (Admin admin = conn.getAdmin()) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            LOG.info("Drop table success.");
        } catch (IOException e) {
            LOG.error("Drop table failed ", e);
        }
    }
}
