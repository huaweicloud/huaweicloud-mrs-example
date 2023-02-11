/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.hbase.util.HBaseConfigurationUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Random;

/**
 * @since 8.2.0
 */
public class WriteHBase {
    private static final Logger LOG = LoggerFactory.getLogger(WriteHBase.class);

    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.WriteHBase"
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
        DataStream<Row> messageStream = env.addSource(new SimpleStringGenerator());
        messageStream.addSink(
                new HBaseWriteSink(paraTool.get("tableName"), createConf(paraTool.get("confDir"))));
        env.execute("WriteHBase");
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

    /**
     * @since 8.2.0
     */
    private static class HBaseWriteSink extends RichSinkFunction<Row> {
        private Connection conn;
        private BufferedMutator bufferedMutator;
        private String tableName;
        private final byte[] serializedConfig;
        private Admin admin;
        private org.apache.hadoop.conf.Configuration hbaseConf;
        private long flushTimeIntervalMillis = 5000; //5s
        private long preFlushTime;

        public HBaseWriteSink(String sourceTable, org.apache.hadoop.conf.Configuration conf) {
            this.tableName = sourceTable;
            this.serializedConfig = HBaseConfigurationUtil.serializeConfiguration(conf);
        }

        private void deserializeConfiguration() {
            LOG.info("Deserialize HBase configuration.");
            hbaseConf = HBaseConfigurationUtil.deserializeConfiguration(
                serializedConfig, HBaseConfigurationUtil.getHBaseConfiguration());
            LOG.info("Deserialization successfully.");
        }

        private void createTable() throws IOException {
            LOG.info("Create HBase Table.");
            if (admin.tableExists(TableName.valueOf(tableName))) {
                LOG.info("Table already exists.");
                return;
            }
            // Specify the table descriptor.
            TableDescriptorBuilder htd = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            // Set the column family name to f1.
            ColumnFamilyDescriptorBuilder hcd = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f1"));
            // Set data encoding methods. HBase provides DIFF,FAST_DIFF,PREFIX
            hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            // Set compression methods, HBase provides two default compression
            // methods:GZ and SNAPPY
            hcd.setCompressionType(Compression.Algorithm.SNAPPY);
            htd.setColumnFamily(hcd.build());
            try {
                admin.createTable(htd.build());
            } catch (IOException e) {
                if (!(e instanceof TableExistsException)
                || !admin.tableExists(TableName.valueOf(tableName))) {
                    throw e;
                }
                LOG.info("Table already exists, ignore.");
            }
            LOG.info("Table created successfully.");
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            LOG.info("Write sink open");
            super.open(parameters);
            deserializeConfiguration();
            conn = ConnectionFactory.createConnection(hbaseConf);
            admin = conn.getAdmin();
            createTable();
            bufferedMutator = conn.getBufferedMutator(TableName.valueOf(tableName));
            preFlushTime = System.currentTimeMillis();
        }

        @Override
        public void close() throws Exception {
            LOG.info("Close HBase Connection.");
            try {
                if (admin != null) {
                    admin.close();
                    admin = null;
                }
                if (bufferedMutator != null) {
                    bufferedMutator.close();
                    bufferedMutator = null;
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
        public void invoke(Row value, Context context) throws Exception {
            LOG.info("Write data to HBase.");
            Put put = new Put(Bytes.toBytes(value.getField(0).toString()));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"), (Bytes.toBytes(value.getField(1).toString())));
            bufferedMutator.mutate(put);

            if (preFlushTime + flushTimeIntervalMillis >= System.currentTimeMillis()) {
                LOG.info("Flush data to HBase.");
                bufferedMutator.flush();
                preFlushTime = System.currentTimeMillis();
                LOG.info("Flush successfully.");
            } else {
                LOG.info("Skip Flush.");
            }

            LOG.info("Write successfully.");
        }
    }

    /**
     * @since 8.2.0
     */
    public static class SimpleStringGenerator implements SourceFunction<Row> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        long i = 0;
        Random random = new Random();

        @Override
        public void run(SourceContext<Row> ctx) throws Exception {
            while (running) {
                Row row = new Row(2);
                row.setField(0, "rk" + random.nextLong());
                row.setField(1, "v" + random.nextLong());
                ctx.collect(row);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
