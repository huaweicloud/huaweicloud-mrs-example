package com.huawei.bigdata.flink.examples;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;

public class WriteToHBase implements OutputFormat<String> {
    private static final Logger LOG = LoggerFactory.getLogger(WriteToHBase.class);

    private org.apache.hadoop.conf.Configuration conf = null;
    private Connection conn = null;
    private Table table = null;

    private static String tableName = "data_test_test";
    private static String columnFamily = "info";
    private static String[] columnName = {"coordinates","ds","roleId","sceneId","server"};

    @Override
    public void configure(Configuration configuration) {
        conf = HBaseConfiguration.create();
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hbase-site.xml");
        LOG.info("finish create configuration successfully...");
    }

    @Override
    public void open(int i, int i1) throws IOException {

        LOG.info("currentUser: {}", UserGroupInformation.getCurrentUser());
        try {
            conn = ConnectionFactory.createConnection(conf);
            LOG.info("connect success");
        } catch (Exception e) {
            LOG.error("connection failed,", e);
        }

        TableName TABLE_NAME = TableName.valueOf(tableName);
        LOG.info("tableName: " + TABLE_NAME);
        table = conn.getTable(TABLE_NAME);
        createTable(TABLE_NAME);
    }

    @Override
    public void writeRecord(String s) {
        putData(s);
    }

    /**
     * Create user info table
     */
    public void createTable(TableName tn) {
        LOG.info("Entering testCreateTable.");

        // Specify the table descriptor.
        HTableDescriptor htd = new HTableDescriptor(tn);

        // Set the column family name to info.
        HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);

        // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
        // and PREFIX_TREE
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);

        // Set compression methods, HBase provides two default compression
        // methods:GZ and SNAPPY
        // GZ has the highest compression rate,but low compression and
        // decompression effeciency,fit for cold data
        // SNAPPY has low compression rate, but high compression and
        // decompression effeciency,fit for hot data.
        // it is advised to use SANPPY
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);

        htd.addFamily(hcd);

        Admin admin = null;
        try {
            // Instantiate an Admin object.
            admin = conn.getAdmin();
            if (!admin.tableExists(tn)) {
                LOG.info("Creating table...");
                admin.createTable(htd);
                LOG.info(admin.getClusterStatus().toString());
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
     * Insert data
     * @param s
     */
    public void putData(String s) {
        LOG.info("Entering testPut.");

        TimeStamp ts = new TimeStamp(new Date());
        Date date = ts.getDate();
        String[] columnValue = s.split("','");

        if(columnValue.length != columnName.length){
            LOG.error("the colume value is not match with the colume name");
        }

        // insert data.
        Put put = new Put(Bytes.toBytes(date.toString()));
        for(int i=0; i<columnName.length; i++){
            if (i == 0) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName[i]),
                        Bytes.toBytes(columnValue[i].substring(1)));
            }

            else if (i == (columnName.length-1)) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName[i]),
                        Bytes.toBytes(columnValue[i].substring(0,columnValue[i].length() -1)));
            }
            else {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName[i]),
                        Bytes.toBytes(columnValue[i]));
            }
        }

        try {
            table.put(put);
            LOG.info("Put successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOG.info("Exiting testPut.");
    }

    @Override
    public void close() {
        try {
            if (table != null) {
                table.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            LOG.error("Close HBase Exception:", e);
        }
    }
}
