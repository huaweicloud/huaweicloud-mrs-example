/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2023. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.huawei.hadoop.security.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.hindex.client.HIndexAdmin;
import org.apache.hadoop.hbase.hindex.client.impl.HIndexClient;
import org.apache.hadoop.hbase.hindex.common.HIndexSpecification;
import org.apache.hadoop.hbase.hindex.common.TableIndices;
import org.apache.hadoop.hbase.hindex.protobuf.generated.HIndexProtos.ColumnQualifier.ValueType;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Function description:
 *
 * HBase Development Instruction Sample Code The sample code uses user
 * information as source data,it introduces how to implement businesss process
 * development using HBase API
 *
 * @since 2020-09-21
 */
public class HBaseSample {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSample.class.getName());

    /**
     * Sample table name
     */
    protected static final String HBASE_SAMPLE_TABLE_NAME = "hbase_sample_table";

    /**
     * Connection between client and server
     */
    protected Connection conn;

    /**
     * Configuration of client
     */
    protected Configuration clientConf;

    /**
     * Sample table name
     */
    protected TableName tableName;

    /**
     * Constructor using default client configuration
     *
     * @throws IOException When the creation fails, an exception is thrown.
     */
    public HBaseSample() throws IOException {
        this(Utils.createClientConf());
    }

    /**
     * Constructor with a client configuration
     *
     * @param clientConf Client configuration
     */
    public HBaseSample(Configuration clientConf) {
        this.tableName = TableName.valueOf(HBASE_SAMPLE_TABLE_NAME);
        this.clientConf = clientConf;
        Utils.handleZkSslEnabled(clientConf);
    }

    /**
     * Create connection between client and hbase server
     *
     * @throws IOException Exception thrown when the connection fails to be created
     */
    public void createConnection() throws IOException {
        this.conn = ConnectionFactory.createConnection(clientConf);
    }

    /**
     * HBaseSample test
     */
    public void test() {
        try {
            testCreateTable();
            testMultiSplit();
            testPut();
            createIndex();
            enableIndex();
            testScanDataByIndex();
            testModifyTable();
            testGet();
            testScanData();
            testSingleColumnValueFilter();
            testFilterList();
            testDelete();
            disableIndex();
            dropIndex();
            dropTable();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (IOException e1) {
                    LOG.error("Failed to close the connection ", e1);
                }
            }
        }
    }

    /**
     * Create user info table
     */
    public void testCreateTable() {
        LOG.info("Entering testCreateTable.");

        // Specify the table descriptor.
        TableDescriptorBuilder htd = TableDescriptorBuilder.newBuilder(tableName);

        // Set the column family name to info.
        ColumnFamilyDescriptorBuilder hcd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));

        // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

        // Set compression methods, HBase provides two default compression
        // methods:GZ and SNAPPY
        // GZ has the highest compression rate,but low compression and
        // decompression effeciency,fit for cold data
        // SNAPPY has low compression rate, but high compression and
        // decompression effeciency,fit for hot data.
        // it is advised to use SANPPY
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);

        htd.setColumnFamily(hcd.build());

        Admin admin = null;
        try {
            // Instantiate an Admin object.
            admin = conn.getAdmin();
            if (!admin.tableExists(tableName)) {
                LOG.info("Creating table...");
                admin.createTable(htd.build());
                LOG.info(admin.getClusterMetrics().toString());
                LOG.info(admin.listNamespaceDescriptors().toString());
                LOG.info("Table created successfully.");
            } else {
                LOG.warn("table already exists");
            }
        } catch (IOException e) {
            LOG.error("Create table failed.", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Failed to close admin ", e);
                }
            }
        }
        LOG.info("Exiting testCreateTable.");
    }

    /**
     * testMultiSplit
     */
    public void testMultiSplit() {
        LOG.info("Entering testMultiSplit.");

        Table table = null;
        Admin admin = null;
        try {
            admin = conn.getAdmin();

            // initilize a HTable object
            table = conn.getTable(tableName);
            Set<RegionInfo> regionSet = new HashSet<RegionInfo>();
            List<HRegionLocation> regionList = conn.getRegionLocator(tableName).getAllRegionLocations();
            for (HRegionLocation hrl : regionList) {
                regionSet.add(hrl.getRegion());
            }
            byte[][] sk = new byte[4][];
            sk[0] = "A".getBytes();
            sk[1] = "D".getBytes();
            sk[2] = "F".getBytes();
            sk[3] = "H".getBytes();
            for (RegionInfo regionInfo : regionSet) {
                admin.multiSplitSync(regionInfo.getRegionName(), sk);
            }
            LOG.info("MultiSplit successfully.");
        } catch (IOException e) {
            LOG.error("MultiSplit failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close table object
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting testMultiSplit.");
    }

    private static Put putData(byte[] familyName, byte[][] qualifiers, List<String> data) {
        Put put = new Put(Bytes.toBytes(data.get(0)));
        put.addColumn(familyName, qualifiers[0], Bytes.toBytes(data.get(1)));
        put.addColumn(familyName, qualifiers[1], Bytes.toBytes(data.get(2)));
        put.addColumn(familyName, qualifiers[2], Bytes.toBytes(data.get(3)));
        put.addColumn(familyName, qualifiers[3], Bytes.toBytes(data.get(4)));
        return put;
    }

    /**
     * Insert data
     */
    public void testPut() {
        LOG.info("Entering testPut.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("info");
        // Specify the column name.
        byte[][] qualifiers = {
            Bytes.toBytes("name"), Bytes.toBytes("gender"), Bytes.toBytes("age"), Bytes.toBytes("address")
        };

        Table table = null;
        try {
            // Instantiate an HTable object.
            table = conn.getTable(tableName);
            List<Put> puts = new ArrayList<Put>();

            // Instantiate a Put object.
            Put put = putData(familyName, qualifiers,
                Arrays.asList("012005000201", "Zhang San", "Male", "19", "Shenzhen, Guangdong"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000202", "Li Wanting", "Female", "23", "Shijiazhuang, Hebei"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000203", "Wang Ming", "Male", "26", "Ningbo, Zhejiang"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000204", "Li Gang", "Male", "18", "Xiangyang, Hubei"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000205", "Zhao Enru", "Female", "21", "Shangrao, Jiangxi"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000206", "Chen Long", "Male", "32", "Zhuzhou, Hunan"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000207", "Zhou Wei", "Female", "29", "Nanyang, Henan"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000208", "Yang Yiwen", "Female", "30", "Kaixian, Chongqing"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000209", "Xu Bing", "Male", "26", "Weinan, Shaanxi"));
            puts.add(put);

            put = putData(familyName, qualifiers,
                Arrays.asList("012005000210", "Xiao Kai", "Male", "25", "Dalian, Liaoning"));
            puts.add(put);

            // Submit a put request.
            table.put(puts);

            LOG.info("Put successfully.");
        } catch (IOException e) {
            LOG.error("Put failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testPut.");
    }

    /**
     * createIndex
     */
    public void createIndex() {
        LOG.info("Entering createIndex.");

        String indexName = "index_name";

        // Create hindex instance
        TableIndices tableIndices = new TableIndices();
        HIndexSpecification iSpec = new HIndexSpecification(indexName);
        iSpec.addIndexColumn(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build(),
            "name", ValueType.STRING);
        tableIndices.addIndex(iSpec);

        HIndexAdmin iAdmin = null;
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            iAdmin = HIndexClient.newHIndexAdmin(admin);

            // add index to the table
            iAdmin.addIndices(tableName, tableIndices);
            LOG.info("Create index successfully.");

        } catch (IOException e) {
            LOG.error("Create index failed.", e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
            if (iAdmin != null) {
                try {
                    // Close IndexAdmin Object
                    iAdmin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting createIndex.");
    }

    /**
     * enableIndex
     */
    public void enableIndex() {
        LOG.info("Entering enableIndex.");

        // Name of the index to be enabled
        String indexName = "index_name";

        List<String> indexNameList = new ArrayList<String>();
        indexNameList.add(indexName);

        HIndexAdmin iAdmin = null;
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            iAdmin = HIndexClient.newHIndexAdmin(admin);

            // Alternately, enable the specified indices
            iAdmin.enableIndices(tableName, indexNameList);
            LOG.info("Successfully enable indices {}  of the table {}", indexNameList, tableName);
        } catch (IOException e) {
            LOG.error("Failed to enable indices {}  of the table {} . {}", indexNameList, tableName, e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
            if (iAdmin != null) {
                try {
                    iAdmin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
    }

    /**
     * disableIndex
     */
    public void disableIndex() {
        LOG.info("Entering disableIndex.");

        // Name of the index to be disabled
        String indexName = "index_name";

        List<String> indexNameList = new ArrayList<String>();
        indexNameList.add(indexName);

        HIndexAdmin iAdmin = null;
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            iAdmin = HIndexClient.newHIndexAdmin(admin);

            // Alternately, enable the specified indices
            iAdmin.disableIndices(tableName, indexNameList);
            LOG.info("Successfully disable indices {}  of the table {}", indexNameList, tableName);
        } catch (IOException e) {
            LOG.error("Failed to disable indices {}  of the table {} . {}", indexNameList, tableName, e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
            if (iAdmin != null) {
                try {
                    iAdmin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
    }

    /**
     * Scan data by secondary index.
     */
    public void testScanDataByIndex() {
        LOG.info("Entering testScanDataByIndex.");

        Table table = null;
        ResultScanner scanner = null;
        try {
            table = conn.getTable(tableName);

            // Create a filter for indexed column.
            Filter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, "Li Gang".getBytes());
            Scan scan = new Scan();
            scan.setFilter(filter);
            scanner = table.getScanner(scan);
            LOG.info("Scan indexed data.");

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
        } finally {
            if (scanner != null) {
                // Close the scanner object.
                scanner.close();
            }
            try {
                if (table != null) {
                    table.close();
                }
            } catch (IOException e) {
                LOG.error("Close table failed ", e);
            }
        }
        LOG.info("Exiting testScanDataByIndex.");
    }

    /**
     * Modify a Table
     */
    public void testModifyTable() {
        LOG.info("Entering testModifyTable.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("education");

        Admin admin = null;
        try {
            // Instantiate an Admin object.
            admin = conn.getAdmin();

            // Obtain the table descriptor.
            TableDescriptor htd = admin.getDescriptor(tableName);

            // Check whether the column family is specified before modification.
            if (!htd.hasColumnFamily(familyName)) {
                // Create the column descriptor.
                TableDescriptor tableBuilder = TableDescriptorBuilder.newBuilder(htd)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(familyName).build())
                    .build();

                // Disable the table to get the table offline before modifying
                // the table.
                admin.disableTable(tableName);
                // Submit a modifyTable request.
                admin.modifyTable(tableBuilder);
                // Enable the table to get the table online after modifying the
                // table.
                admin.enableTable(tableName);
            }
            LOG.info("Modify table successfully.");
        } catch (IOException e) {
            LOG.error("Modify table failed ", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting testModifyTable.");
    }

    /**
     * Get Data
     */
    public void testGet() {
        LOG.info("Entering testGet.");

        // Specify the column family name.
        byte[] familyName = Bytes.toBytes("info");
        // Specify the column name.
        byte[][] qualifier = {Bytes.toBytes("name"), Bytes.toBytes("address")};
        // Specify RowKey.
        byte[] rowKey = Bytes.toBytes("012005000201");

        Table table = null;
        try {
            // Create the Configuration instance.
            table = conn.getTable(tableName);

            // Instantiate a Get object.
            Get get = new Get(rowKey);

            // Set the column family name and column name.
            get.addColumn(familyName, qualifier[0]);
            get.addColumn(familyName, qualifier[1]);

            // Submit a get request.
            Result result = table.get(get);

            // Print query results.
            for (Cell cell : result.rawCells()) {
                LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                    Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                    Bytes.toString(CellUtil.cloneValue(cell)));
            }
            LOG.info("Get data successfully.");
        } catch (IOException e) {
            LOG.error("Get data failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testGet.");
    }

    /**
     * testScanData
     */
    public void testScanData() {
        LOG.info("Entering testScanData.");

        Table table = null;
        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;
        try {
            // Create the Configuration instance.
            table = conn.getTable(tableName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Set the cache size.
            scan.setCaching(1000);

            // Submit a scan request.
            rScanner = table.getScanner(scan);

            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Scan data successfully.");
        } catch (IOException e) {
            LOG.error("Scan data failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testScanData.");
    }

    /**
     * testSingleColumnValueFilter
     */
    public void testSingleColumnValueFilter() {
        LOG.info("Entering testSingleColumnValueFilter.");

        Table table = null;

        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;

        try {
            // Create the Configuration instance.
            table = conn.getTable(tableName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Set the filter criteria.
            SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("name"),
                CompareOperator.EQUAL, Bytes.toBytes("Xu Bing"));

            scan.setFilter(filter);

            // Submit a scan request.
            rScanner = table.getScanner(scan);

            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Single column value filter successfully.");
        } catch (IOException e) {
            LOG.error("Single column value filter failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testSingleColumnValueFilter.");
    }

    /**
     * testFilterList
     */
    public void testFilterList() {
        LOG.info("Entering testFilterList.");

        Table table = null;

        // Instantiate a ResultScanner object.
        ResultScanner rScanner = null;

        try {
            // Create the Configuration instance.
            table = conn.getTable(tableName);

            // Instantiate a Get object.
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

            // Instantiate a FilterList object in which filters have "and"
            // relationship with each other.
            FilterList list = new FilterList(Operator.MUST_PASS_ALL);
            // Obtain data with age of greater than or equal to 20.
            list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"),
                CompareOperator.GREATER_OR_EQUAL,
                Bytes.toBytes(Long.valueOf(20))));
            // Obtain data with age of less than or equal to 29.
            list.addFilter(
                new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("age"), CompareOperator.LESS_OR_EQUAL,
                    Bytes.toBytes(Long.valueOf(29))));

            scan.setFilter(list);

            // Submit a scan request.
            rScanner = table.getScanner(scan);
            // Print query results.
            for (Result r = rScanner.next(); r != null; r = rScanner.next()) {
                for (Cell cell : r.rawCells()) {
                    LOG.info("{}:{},{},{}", Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            LOG.info("Filter list successfully.");
        } catch (IOException e) {
            LOG.error("Filter list failed ", e);
        } finally {
            if (rScanner != null) {
                // Close the scanner object.
                rScanner.close();
            }
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testFilterList.");
    }

    /**
     * deleting data
     */
    public void testDelete() {
        LOG.info("Entering testDelete.");

        byte[] rowKey = Bytes.toBytes("012005000201");

        Table table = null;
        try {
            // Instantiate an HTable object.
            table = conn.getTable(tableName);

            // Instantiate an Delete object.
            Delete delete = new Delete(rowKey);

            // Submit a delete request.
            table.delete(delete);

            LOG.info("Delete table successfully.");
        } catch (IOException e) {
            LOG.error("Delete table failed ", e);
        } finally {
            if (table != null) {
                try {
                    // Close the HTable object.
                    table.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }
        }
        LOG.info("Exiting testDelete.");
    }

    /**
     * dropIndex
     */
    public void dropIndex() {
        LOG.info("Entering dropIndex.");

        String indexName = "index_name";
        List<String> indexNameList = new ArrayList<String>();
        indexNameList.add(indexName);

        HIndexAdmin iAdmin = null;
        try {
            // Instantiate HIndexAdmin Object
            iAdmin = HIndexClient.newHIndexAdmin(conn.getAdmin());

            // Delete Secondary Index
            iAdmin.dropIndices(tableName, indexNameList);

            LOG.info("Drop index successfully.");
        } catch (IOException e) {
            LOG.error("Drop index failed ", e);
        } finally {
            if (iAdmin != null) {
                try {
                    // Close Secondary Index
                    iAdmin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting dropIndex.");
    }

    /**
     * Delete user table
     */
    public void dropTable() {
        LOG.info("Entering dropTable.");

        Admin admin = null;
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(tableName)) {
                // Disable the table before deleting it.
                admin.disableTable(tableName);

                // Delete table.
                admin.deleteTable(tableName);
            }
            LOG.info("Drop table successfully.");
        } catch (IOException e) {
            LOG.error("Drop table failed ", e);
        } finally {
            if (admin != null) {
                try {
                    // Close the Admin object.
                    admin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting dropTable.");
    }

    /**
     * grantACL
     */
    public void grantACL() {
        LOG.info("Entering grantACL.");

        String user = "huawei";
        String permissions = "RW";

        String familyName = "info";
        String qualifierName = "name";

        Table mt = null;
        Admin hAdmin = null;
        try {
            // Create ACL Instance
            mt = conn.getTable(PermissionStorage.ACL_TABLE_NAME);

            Permission perm = new Permission(Bytes.toBytes(permissions));

            hAdmin = conn.getAdmin();
            TableDescriptor ht = hAdmin.getDescriptor(tableName);

            // Judge whether the table exists
            if (hAdmin.tableExists(mt.getName())) {
                // Judge whether ColumnFamily exists
                if (ht.hasColumnFamily(Bytes.toBytes(familyName))) {
                    // grant permission
                    AccessControlClient.grant(conn, tableName, user, Bytes.toBytes(familyName),
                        (qualifierName == null ? null : Bytes.toBytes(qualifierName)), perm.getActions());
                } else {
                    // grant permission
                    AccessControlClient.grant(conn, tableName, user, null, null, perm.getActions());
                }
            }
            LOG.info("Grant ACL successfully.");
        } catch (Throwable e) {
            LOG.error("Grant ACL failed ", e);
        } finally {
            if (mt != null) {
                try {
                    // Close
                    mt.close();
                } catch (IOException e) {
                    LOG.error("Close table failed ", e);
                }
            }

            if (hAdmin != null) {
                try {
                    // Close Admin Object
                    hAdmin.close();
                } catch (IOException e) {
                    LOG.error("Close admin failed ", e);
                }
            }
        }
        LOG.info("Exiting grantACL.");
    }
}
