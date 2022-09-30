/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * @since 8.2.0
 */
public class ReadHBase {
    private static final Logger LOG = LoggerFactory.getLogger(ReadHBase.class);

    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.ReadHBase"
                        + " /opt/test.jar --tableName t1 --confDir /tmp/hbaseConf");

        System.out.println(
                "******************************************************************************************");
        System.out.println("<tableName> hbase tableName");
        System.out.println("<confDir> hbase conf dir");
        System.out.println(
                "******************************************************************************************");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        DataStream<Row> messageStream =
                env.addSource(
                        new HBaseReaderSource(
                                paraTool.get("tableName"), createConf(paraTool.get("confDir"))));
        messageStream
                .rebalance()
                .map(
                        new MapFunction<Row, String>() {
                            @Override
                            public String map(Row s) throws Exception {
                                return "Flink says " + s + System.getProperty("line.separator");
                            }
                        })
                .print();
        env.execute("ReadHBase");
    }

    private static org.apache.hadoop.conf.Configuration createConf(String confDir) {
        LOG.info("Create HBase configuration.");
        org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfigurationUtil.getHBaseConfiguration();
        if (confDir != null) {
            File hbaseSite = new File(confDir + File.separator + "hbase-site.xml");
            if (hbaseSite.exists()) {
                LOG.info("Add hbase-site.xml");
                hbaseConf.addResource(new Path(hbaseSite.getPath()));
            }
            File coreSite = new File(confDir + File.separator + "core-site.xml");
            if (coreSite.exists()) {
                LOG.info("Add core-site.xml");
                hbaseConf.addResource(new Path(coreSite.getPath()));
            }
            File hdfsSite = new File(confDir + File.separator + "hdfs-site.xml");
            if (hdfsSite.exists()) {
                LOG.info("Add hdfs-site.xml");
                hbaseConf.addResource(new Path(hdfsSite.getPath()));
            }
        }
        LOG.info("HBase configuration created successfully.");
        return hbaseConf;
    }

    private static class HBaseReaderSource extends RichSourceFunction<Row> {

        private Connection conn;
        private Table table;
        private Scan scan;
        private String tableName;
        private final byte[] serializedConfig;
        private Admin admin;
        private org.apache.hadoop.conf.Configuration hbaseConf;

        public HBaseReaderSource(String sourceTable, org.apache.hadoop.conf.Configuration conf) {
            this.tableName = sourceTable;
            this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(conf);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            LOG.info("Read source open");
            super.open(parameters);
            deserializeConfiguration();
            conn = ConnectionFactory.createConnection(hbaseConf);
            admin = conn.getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                throw new IOException("table does not exist.");
            }
            table = conn.getTable(TableName.valueOf(tableName));
            scan = new Scan();
        }

        private void deserializeConfiguration() {
            LOG.info("Deserialize HBase configuration.");
            hbaseConf = HBaseConfigurationUtil.deserializeConfiguration(
                serializedConfig, HBaseConfigurationUtil.getHBaseConfiguration());
            LOG.info("Deserialization successfully.");
        }

        @Override
        public void run(SourceContext<Row> sourceContext) throws Exception {
            LOG.info("Read source run");
            try (ResultScanner scanner = table.getScanner(scan)) {
                Iterator<Result> iterator = scanner.iterator();
                while (iterator.hasNext()) {
                    Result result = iterator.next();
                    String rowKey = Bytes.toString(result.getRow());
                    byte[] value = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
                    Row row = new Row(2);
                    row.setField(0, rowKey);
                    row.setField(1, Bytes.toString(value));
                    sourceContext.collect(row);
                    LOG.info("Send data successfully.");
                }
            }

            LOG.info("Read successfully.");
        }

        @Override
        public void close() throws Exception {
            closeHBase();
        }

        private void closeHBase() {
            LOG.info("Close HBase Connection.");
            try {
                if (admin != null) {
                    admin.close();
                    admin = null;
                }
                if (table != null) {
                    table.close();
                    table = null;
                }
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (IOException e) {
                LOG.error("Close HBase Exception:", e);
                throw new RuntimeException(e);
            }
            LOG.info("Close successfully.");
        }

        @Override
        public void cancel() {
            closeHBase();
        }
    }
}
