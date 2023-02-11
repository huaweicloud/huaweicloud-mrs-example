/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.huawei.hadoop.security.LoginUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Function description:
 *
 * hbase-thrift-example test main class
 *
 * @since 2013
 */

public class TestMain {
    private static final  Logger LOG = LoggerFactory.getLogger(TestMain.class.getName());

    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";

    private static Configuration conf = null;

    private static String krb5File = null;

    private static String userName = null;

    private static String userKeytabFile = null;

    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyyMMdd");

    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static int THRIFT_PORT = 21305;


    public static void main(String[] args) {
        try {
            init();
            login();
        } catch (IOException e) {
            e.printStackTrace();
        }
        ThriftSample test = null;
        try {
            test = new ThriftSample();
            test.test("xxx.xxx.xxx.xxx", THRIFT_PORT, conf);
        } catch (TException | IOException e) {
            LOG.error("Test thrift error", e);
        }
        LOG.info("-----------finish Thrift -------------------");
    }

    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            userName = "hbaseuser1";

            //In Windows environment
            String userdir = TestMain.class.getClassLoader().getResource("conf").getPath() + File.separator;
            //In Linux environment
            //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            
            userKeytabFile = userdir + "user.keytab";
            krb5File = userdir + "krb5.conf";
            /*
             * if need to connect zk, please provide jaas info about zk. of course,
             * you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath +
             * "jaas.conf"); but the demo can help you more : Note: if this process
             * will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    private static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        String userdir = TestMain.class.getClassLoader().getResource("conf").getPath() + File.separator;
        //In Linux environment
        //String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"), false);
        conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
        conf.addResource(new Path(userdir + "hbase-site.xml"), false);
    }

    private static class WriteCallable implements Callable<Long> {
        private String tablePrefix;

        private long checkPeriod = 5 * 1000;

        private final Connection conn;

        private final Random random;

        public WriteCallable(String tablePrefix, Connection conn) {
            this.tablePrefix = tablePrefix;
            this.conn = conn;
            random = new Random();
        }

        @Override
        public Long call() throws Exception {
            TableName tableName = null;
            long nextCheckPeriod = 0;
            while (tableName == null) {
                if (COUNTER.get() > 0) {
                    tableName = TableName.valueOf(tablePrefix + FORMATTER.format(new Date()));
                    nextCheckPeriod = System.currentTimeMillis() + checkPeriod;
                    break;
                }
            }
            Table table = null;
            long totalRows = 0;
            long totalCost = 0;
            try {
                table = conn.getTable(tableName);
                while (true && totalRows < 999999999) {
                    if (COUNTER.get() > 0 && nextCheckPeriod < System.currentTimeMillis()) {
                        TableName tmpTableName = TableName.valueOf(tablePrefix + FORMATTER.format(new Date()));
                        if (!tableName.equals(tmpTableName)) {
                            tableName = tmpTableName;
                            table.close();
                            table = conn.getTable(tableName);
                            LOG.info("Update to table {}", tableName);
                            COUNTER.decrementAndGet();
                        }
                    }
                    long start = System.currentTimeMillis();
                    table.put(generatePutList(100));
                    totalCost += (System.currentTimeMillis() - start);
                    totalRows += 100;
                    if (totalRows % 10000 == 0) {
                        LOG.info("Wrote {} rows in {}ms, mean time: {}.", totalRows, totalCost, totalCost / 10000);
                        totalCost = 0;
                    }
                }
                return totalRows;
            } finally {
                if (table != null) {
                    table.close();
                }
            }
        }

        private List<Put> generatePutList(int rows) {
            List<Put> putList = new ArrayList<>(rows);
            for (int i = 0; i < rows; i++) {
                putList.add(generatePut());
            }
            return putList;
        }

        private Put generatePut() {
            StringBuilder sb = new StringBuilder().append(System.currentTimeMillis())
                .reverse()
                .append("_")
                .append(MD5Hash.getMD5AsHex(Bytes.toBytes(random.nextLong())));
            Put put = new Put(Bytes.toBytes(sb.toString()));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q1"),
                Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(random.nextDouble()))));
            put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("q2"),
                Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(random.nextDouble()))));
            put.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("q1"),
                Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(random.nextDouble()))));
            return put;
        }

    }
}
