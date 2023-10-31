/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.huawei.hadoop.security.Utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.hindex.global.GlobalIndexAdmin;
import org.apache.hadoop.hbase.hindex.global.TableIndices;
import org.apache.hadoop.hbase.hindex.global.common.HIndexSpecification;
import org.apache.hadoop.hbase.hindex.global.common.IndexState;
import org.apache.hadoop.hbase.hindex.global.common.ValueType;
import org.apache.hadoop.hbase.hindex.global.impl.GlobalIndexClient;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase Global Secondary Index Sample by using APIs.
 * In this example we will create a user table and create a covered global secondary index on it.
 * 1.Create user table
 * The user table is 'user_table', has one column family 'info'.
 * We will put data to these qualifiers:
 * --------------------------------------
 * rowkey info:id info:age info:name info:address
 * r1   1   20  Zhang CityA
 * r2   2   30  Li CityB
 * r3   3   35  Wang CityC
 * ---------------------------------------
 * 2.Create global index
 * Define the index columns: 'info:id' and 'info:age';
 * these qualifier values will use to generate index data row key like: "id_value,age_value"
 * Define the covered columns: 'info:name';
 * these qualifier will also store in index table.
 * ---------------------------------------
 * 3.Query by index
 * Define the query condition: query the record which id=3, define a filter used for user table scan:
 * SingleColumnValueFilter('info','id',=,'binary:3');
 * If our query condition suit index define, will convert user table scan to a range scan for index table to speed up query.
 *
 * @since 2023-08-22
 */
public class GlobalSecondaryIndexSample {
    private static final Logger LOG = LoggerFactory.getLogger(GlobalSecondaryIndexSample.class);

    private final TableName tableName = TableName.valueOf("user_table");

    private final Connection conn;
    
    public GlobalSecondaryIndexSample() throws IOException {
        Configuration gsiClientConf = HBaseConfiguration.create(Utils.createClientConf());
        // If you want to use global secondary index in client, must set "hbase.client.gsi.cache.enabled" to true.
        gsiClientConf.setBoolean("hbase.client.gsi.cache.enabled", true);
        Utils.handleZkSslEnabled(gsiClientConf);
        this.conn = ConnectionFactory.createConnection(gsiClientConf);
    }

    /**
     * GlobalSecondaryIndexSample test
     */
    public void test() {
        try {
            createTable();
            testCreateIndex();
            testListIndexes();
            testPut();
            testScanDataByIndex();
            testDelete();
            testAlterIndexUnusable();
            testDropIndex();
            dropTable();
        } finally {
            IOUtils.closeQuietly(conn, e -> LOG.error("Failed to close the connection ", e));
        }
    }

    /**
     * Create user table
     */
    public void createTable() {
        LOG.info("Entering createTable.");
        try (Admin admin = conn.getAdmin()) {
            if (admin.tableExists(tableName)) {
                LOG.warn("table already exists");
                return;
            }

            LOG.info("Creating table...");

            // create 'user_table','info'
            ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"))
                .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                .setCompressionType(Compression.Algorithm.SNAPPY)
                .build();
            TableDescriptor htd = TableDescriptorBuilder.newBuilder(tableName)
                .setColumnFamily(cfd)
                .build();

            // Generate user table split keys
            // Assume our row key like r0 r1 r2...
            byte[][] splitKeys = new byte[10][];
            for (int i = 0; i < 10; i++) {
                splitKeys[i] = Bytes.toBytesBinary("r" + i);
            }

            admin.createTable(htd, splitKeys);
            LOG.info("Table created successfully.");
        } catch (IOException e) {
            LOG.error("Create table failed.", e);
        }
        LOG.info("Exiting createTable.");
    }

    /**
     * createIndex
     * If create index on a no-empty user table, the index state will be INACTIVE,
     * need use GlobalTableIndexer to build index data then to use it.
     */
    public void testCreateIndex() {
        LOG.info("Entering createIndex.");
        // Create index instance
        TableIndices tableIndices = new TableIndices();
        // Create index spec
        // idx_id_age covered info:name
        HIndexSpecification indexSpec = new HIndexSpecification("idx_id_age");

        // Set index column
        indexSpec.addIndexColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), ValueType.STRING);
        indexSpec.addIndexColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), ValueType.STRING);

        // Set covered column
        // If you want cover one column, use addCoveredColumn
        // If you want cover all column in one column family, use addCoveredFamilies
        // If you want cover all column of all column family, use setCoveredAllColumns
        indexSpec.addCoveredColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

        // Need specify index table split keys, it should specify by index column.
        // Note: index data's row key has a same prefix "\x01"
        // For example:
        // Our index column include "id" and "age", "id" is a number, we
        // could specify split key like \x010 \x011 \x012...
        byte[][] splitKeys = new byte[10][];
        for (int i = 0; i < 10; i++) {
            splitKeys[i] = Bytes.toBytesBinary("\\x01" + i);
        }
        indexSpec.setSplitKeys(splitKeys);
        tableIndices.addIndex(indexSpec);

        // iAdmin will close the inner admin instance
        try (GlobalIndexAdmin iAdmin = GlobalIndexClient.newIndexAdmin(conn.getAdmin())) {
            // add index to the table
            iAdmin.addIndices(tableName, tableIndices);
            LOG.info("Create index successfully.");
        } catch (IOException e) {
            LOG.error("Create index failed.", e);
        }
        LOG.info("Exiting createIndex.");
    }

    /**
     * List indexes
     */
    public void testListIndexes() {
        LOG.info("Entering testListIndexes.");
        try (GlobalIndexAdmin iAdmin = GlobalIndexClient.newIndexAdmin(conn.getAdmin())) {
            for (Pair<HIndexSpecification, IndexState> indexPair : iAdmin.listIndices(tableName)) {
                LOG.info("index spec:{}, index state:{}", indexPair.getFirst(), indexPair.getSecond());
            }
            LOG.info("List indexes successfully.");
        } catch (IOException e) {
            LOG.error("List indexes failed.", e);
        }
        LOG.info("Exiting testListIndexes.");
    }

    /**
     * Insert data
     */
    public void testPut() {
        LOG.info("Entering testPut.");

        List<Put> puts = new ArrayList<>();
        Put put1 = new Put(Bytes.toBytes("r1"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(1)));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(20)));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Zhang"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("CityA"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("r2"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(2)));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(30)));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Li"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("CityB"));
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("r3"));
        put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(3)));
        put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(35)));
        put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Wang"));
        put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("address"), Bytes.toBytes("CityC"));
        puts.add(put3);

        try (Table table = conn.getTable(tableName)) {
            table.put(puts);
            LOG.info("Put successfully.");
        } catch (IOException e) {
            LOG.error("Put failed ", e);
        }
        LOG.info("Exiting testPut.");
    }

    /**
     * Scan data by secondary index.
     */
    public void testScanDataByIndex() {
        LOG.info("Entering testScanDataByIndex.");

        Scan scan = new Scan();
        // Create a filter for indexed column.
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("id"),
            CompareOperator.EQUAL, Bytes.toBytes("3"));
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);

        // Specify returned columns
        // If returned columns not included in index table, will query back user table,
        // it's not the fast way to get data, suggest to cover all needed columns.
        // If you want to confirm whether using index for scanning, please set hbase client log level to DEBUG.
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
        scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

        LOG.info("Scan indexed data.");
        try (Table table = conn.getTable(tableName); ResultScanner scanner = table.getScanner(scan)) {
            for (Result result : scanner) {
                for (Cell cell : result.rawCells()) {
                    LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Scan data by index successfully.");
        } catch (IOException e) {
            LOG.error("Scan data by index failed ", e);
        }
        LOG.info("Exiting testScanDataByIndex.");
    }

    /**
     * deleting data
     */
    public void testDelete() {
        LOG.info("Entering testDelete.");
        byte[] rowKey = Bytes.toBytes("3");
        try (Table table = conn.getTable(tableName)) {
            Delete delete = new Delete(rowKey);
            table.delete(delete);
            LOG.info("Delete successfully.");
        } catch (IOException e) {
            LOG.error("Delete failed ", e);
        }
        LOG.info("Exiting testDelete.");
    }

    /**
     * alter index to UNUSABLE state.
     */
    public void testAlterIndexUnusable() {
        LOG.info("Entering testAlterIndexUnusable.");
        List<String> indexNameList = Lists.newArrayList("idx_id_age");
        // Instantiate GlobalIndexAdmin Object
        try (GlobalIndexAdmin iAdmin = GlobalIndexClient.newIndexAdmin(conn.getAdmin())) {
            iAdmin.alterGlobalIndicesUnusable(tableName, indexNameList);
            LOG.info("Alter indices to UNUSABLE successfully.");
        } catch (IOException e) {
            LOG.error("Alter indices to UNUSABLE failed.", e);
        }
        LOG.info("Exiting testAlterIndexUnusable.");
    }

    /**
     * dropIndex
     */
    public void testDropIndex() {
        LOG.info("Entering testDropIndex.");
        List<String> indexNameList = Lists.newArrayList("idx_id_age");
        // Instantiate HIndexAdmin Object
        try (GlobalIndexAdmin iAdmin = GlobalIndexClient.newIndexAdmin(conn.getAdmin())) {
            // Delete Secondary Index
            iAdmin.dropIndices(tableName, indexNameList);
            LOG.info("Drop index successfully.");
        } catch (IOException e) {
            LOG.error("Drop index failed ", e);
        }
        LOG.info("Exiting testDropIndex.");
    }

    /**
     * Delete user table
     */
    public void dropTable() {
        LOG.info("Entering dropTable.");
        try (Admin admin = conn.getAdmin()) {
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            LOG.info("Drop table successfully.");
        } catch (IOException e) {
            LOG.error("Drop table failed ", e);
        }
        LOG.info("Exiting dropTable.");
    }
}
