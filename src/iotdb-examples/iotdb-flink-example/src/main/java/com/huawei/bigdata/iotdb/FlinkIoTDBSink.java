/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import com.google.common.collect.Lists;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.iotdb.flink.DefaultIoTSerializationSchema;
import org.apache.iotdb.flink.IoTDBSink;
import org.apache.iotdb.flink.IoTSerializationSchema;
import org.apache.iotdb.flink.options.IoTDBSinkOptions;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * This example shows a case that sends data to a IoTDB server from a Flink job
 *
 * @since 2021-07-28
 */
public class FlinkIoTDBSink {
  private static IoTDBProperties iotdbProps = IoTDBProperties.getInstance();
  /**
   * set truststore.jks path only when iotdb_ssl_enable is true.
   * if modify iotdb_ssl_enable to false, modify IoTDB client's iotdb_ssl_enable="false" in iotdb-client.env,
   * iotdb-client.env file path: /opt/client/IoTDB/iotdb/conf
   */
  private static String IOTDB_SSL_ENABLE = iotdbProps.getIotdb_ssl_enable();
  private static String IOTDB_SSL_TRUSTSTORE = iotdbProps.getIotdb_ssl_truststore();
  private static String HOST = iotdbProps.getLocal_host();
  private static String PORT = iotdbProps.getLocal_port();
  private static String USER = iotdbProps.getUsername();
  private static String PASSWORD = iotdbProps.getPassword();

  public static void main(String[] args) throws Exception {
    // print comment for command to use run flink
    System.out.println("use command as: ");
    System.out.println(
        "./bin/flink run --class com.huawei.bigdata.iotdb.FlinkIoTDBSink"
            + " -m yarn-cluster -yt ssl/ -yt /opt/client/Flink/flink/conf/iotdb-example.properties "
            + "/opt/client/Flink/flink/conf/iotdb-flink-example.jar ");
    System.out.println(
        "******************************************************************************************");

    // run the flink job on local mini cluster
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // set iotdb_ssl_enable
    System.setProperty("iotdb_ssl_enable", IOTDB_SSL_ENABLE);
    if ("true".equals(IOTDB_SSL_ENABLE)) {
      // set truststore.jks path
      System.setProperty("iotdb_ssl_truststore", IOTDB_SSL_TRUSTSTORE);
    }

    IoTDBSinkOptions options = new IoTDBSinkOptions();
    options.setHost(HOST);
    options.setPort(Integer.parseInt(PORT));
    options.setUser(USER);
    options.setPassword(PASSWORD);

    // If the server enables auto_create_schema, then we do not need to register all timeseries
    // here.
    options.setTimeseriesOptionList(
            Lists.newArrayList(
                    new IoTDBSinkOptions.TimeseriesOption(
                            "root.sg.d1.s1", TSDataType.DOUBLE, TSEncoding.GORILLA, CompressionType.SNAPPY)));

    IoTSerializationSchema serializationSchema = new DefaultIoTSerializationSchema();
    IoTDBSink ioTDBSink =
        new IoTDBSinkSSL(options, serializationSchema)
            // enable batching
            .withBatchSize(10)
            // how many connectons to the server will be created for each parallelism
            .withSessionPoolSize(3);

    env.addSource(new SensorSource())
        .name("sensor-source")
        .setParallelism(1)
        .addSink(ioTDBSink)
        .name("iotdb-sink");

    env.execute("iotdb-flink-example");
  }

  private static class SensorSource implements SourceFunction<Map<String, String>> {
    boolean running = true;
    int count = 20;
    Random random = new SecureRandom();

    @Override
    public void run(SourceContext context) throws Exception {
      while (count > 0) {
        count--;
        Map<String, String> tuple = new HashMap();
        tuple.put("device", "root.sg.d1");
        tuple.put("timestamp", String.valueOf(System.currentTimeMillis()));
        tuple.put("measurements", "s1");
        tuple.put("types", "DOUBLE");
        tuple.put("values", String.valueOf(random.nextDouble()));

        context.collect(tuple);
        Thread.sleep(1000);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }
  }
}
