/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2022. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import org.apache.iotdb.flink.options.IoTDBSourceOptions;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.List;

/**
 * This example shows a case where data are read from IoTDB
 *
 * @since 2021-07-28
 */
public class FlinkIoTDBSource {
  /**
   * set truststore.jks path only when iotdb_ssl_enable is true.
   * if modify iotdb_ssl_enable to false, modify IoTDB client's iotdb_ssl_enable="false" in iotdb-client.env,
   * iotdb-client.env file path: /opt/client/IoTDB/iotdb/conf
   */
  public static final String IOTDB_SSL_ENABLE = "true";
  static final String LOCAL_HOST = "127.0.0.1";
  static final int LOCAL_PORT = 22260;
  static final String USER_NAME = "IoTDB登录用户名";
  static final String USER_PW = "IoTDB登录密码";

  static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  static final String ROOT_SG1_D1 = "root.sg1.d1";

  public static void main(String[] args) throws Exception {
    // use session api to create data in IoTDB
    prepareData();

    // run the flink job on local mini cluster
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    IoTDBSourceOptions ioTDBSourceOptions =
        new IoTDBSourceOptions(
            LOCAL_HOST, LOCAL_PORT, USER_NAME, USER_PW, "select s1 from " + ROOT_SG1_D1 + " align by device");


    IoTDBSourceSSL<RowRecord> source =
        new IoTDBSourceSSL<RowRecord>(ioTDBSourceOptions) {
          @Override
          public RowRecord convert(RowRecord rowRecord) {
            return rowRecord;
          }
        };
    env.addSource(source).name("sensor-source").print().setParallelism(2);
    env.execute();
  }

  private static void prepareData()
      throws IoTDBConnectionException, StatementExecutionException, TTransportException {
    // set iotdb_ssl_enable
    System.setProperty("iotdb_ssl_enable", IOTDB_SSL_ENABLE);
    if ("true".equals(IOTDB_SSL_ENABLE)) {
      // set truststore.jks path, this file need to copy to FlinkResource Node
      System.setProperty("iotdb_ssl_truststore", "truststore文件路径");
    }

    Session session = new Session(LOCAL_HOST, LOCAL_PORT, USER_NAME, USER_PW);
    session.open(false);
    try {
      session.setStorageGroup("root.sg1");
      if (!session.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
        session.createTimeseries(
            ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
        List<String> measurements = new ArrayList<>();
        measurements.add("s1");
        measurements.add("s2");
        measurements.add("s3");
        List<TSDataType> types = new ArrayList<>();
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);
        types.add(TSDataType.INT64);

        for (long time = 0; time < 1000; time++) {
          List<Object> values = new ArrayList<>();
          values.add(1L);
          values.add(2L);
          values.add(3L);
          session.insertRecord(ROOT_SG1_D1, time, measurements, types, values);
        }
      }
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST.getStatusCode()) {
        throw e;
      }
    }
  }
}
