package com.huawei.bigdata.createTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CreateTable {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTable.class);
    public  void InitHBase(String[] tableName,Configuration conf) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        System.out.println("---------------tables:"+tableName.length);
        LOG.info("---------------tables:"+tableName.length);
        for(int i = 0;i<tableName.length;i++) {
            TableName tablename = TableName.valueOf(tableName[i]);
            if (!admin.tableExists(tablename)) {
                CreateTableHBase(admin, tablename);
            } else {
                System.out.println("thisMonthTable already exists");
                LOG.info("thisMonthTable already exists");
            }
        }
    }
    public void CreateTableHBase(Admin admin, TableName tableName) {
        System.out.println("Creating Table..." + tableName);
        LOG.info("Creating Table..." + tableName);
        // 表的描述类
        HTableDescriptor htd = new HTableDescriptor(tableName);
        // 列组的描述
        HColumnDescriptor hcd = new HColumnDescriptor("base");
        // 压缩方式
        hcd.setCompressionType(Compression.Algorithm.SNAPPY);
        // 编码方式
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        htd.addFamily(hcd);
        try {
            // 指定起止RowKey和region个数；此时的起始RowKey为第一个region的endKey，结束key为最后一个region的startKey。
            admin.createTable(htd, Bytes.toBytes(String.format("%03d", 1)), Bytes.toBytes(String.valueOf(199)), 200);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
