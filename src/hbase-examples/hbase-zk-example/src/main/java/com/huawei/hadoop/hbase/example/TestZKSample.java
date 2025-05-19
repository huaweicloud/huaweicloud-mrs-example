/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.hadoop.hbase.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Locale;

/**
 * Function description:
 *
 * hbase zk sample test class
 *
 * @since 2013
 */
public class TestZKSample {
    private static final Logger LOG = LoggerFactory.getLogger(TestZKSample.class.getName());

    private static Configuration conf = null;
    private TableName tableName = null;
    private Connection conn = null;

    /**
     * HBase zk Sample test
     */
    public void testSample() throws IOException {
        String keytabFile = "user.keytab";
        String principal = "hbaseuser1";
        tableName = TableName.valueOf("hbase_zk_sample_table");

        login(keytabFile, principal);
        conn = ConnectionFactory.createConnection(conf);
        // HBase connect huawei zookeeper
        createTable(conf);
        dropTable();

        // HBase connect apache zookeeper
        try {
            connectApacheZK();
        } catch(IOException | org.apache.zookeeper.KeeperException e) {
            LOG.error("Connect Apache Zk failed.", e);
        }
    }

    private void connectApacheZK() throws IOException, org.apache.zookeeper.KeeperException {
        try {
            // Create apache zookeeper connection.
            ZooKeeper digestZk = new ZooKeeper("127.0.0.1:2181", 60000, null);
            LOG.info("digest directory：{}", digestZk.getChildren("/", null));
            LOG.info("Successfully connect to apache zookeeper.");
        } catch (InterruptedException e) {
            LOG.error("Found error when connect apache zookeeper ", e);
        }
    }

    private static void login(String keytabFile, String principal) throws IOException {
        conf = HBaseConfiguration.create();
        // Set zoo.cfg for hbase to connect to fi zookeeper.
        String confDirPath;
        String osName = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        if (osName.contains("windows")) {
            // In Windows environment
            confDirPath = TestZKSample.class.getClassLoader().getResource("").getPath() + File.separator;
            // For JDK 9+, not support the path starts with /, need remove it.
            if (confDirPath.startsWith("/")) {
                int length = confDirPath.length();
                confDirPath = confDirPath.substring(1, length);
            }
        } else {
            // In Linux environment
            confDirPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        }
        conf.set("hbase.client.zookeeper.config.path", confDirPath + "zoo.cfg");
        if (User.isHBaseSecurityEnabled(conf)) {
            // jaas.conf file, it is included in the client pakcage file
            System.setProperty("java.security.auth.login.config", confDirPath + "jaas.conf");
            // set the kerberos server info,point to the kerberosclient
            System.setProperty("java.security.krb5.conf", confDirPath + "krb5.conf");
            // set the keytab file name
            conf.set("username.client.keytab.file", confDirPath + keytabFile);
            // set the user's principal
            try {
                conf.set("username.client.kerberos.principal", principal);
                User.login(conf, "username.client.keytab.file", "username.client.kerberos.principal",
                    InetAddress.getLocalHost().getCanonicalHostName());
            } catch (IOException e) {
                throw new IOException("Login failed.", e);
            }
        }
    }

    /**
     * Create user info table
     */
    public void createTable(Configuration conf) {
        LOG.info("Entering testCreateTable.");

        // Specify the table descriptor.
        TableDescriptorBuilder htd = TableDescriptorBuilder.newBuilder(tableName);

        // Set the column family name to info.
        ColumnFamilyDescriptorBuilder hcd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));

        // Set data encoding methods，HBase provides DIFF,FAST_DIFF,PREFIX
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

    public static void main(String[] args) {
        TestZKSample ts = new TestZKSample();
        try {
            ts.testSample();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

