/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * IoTDB Session Pool Example
 *
 * @since 2021-06-15
 */
public class SessionPoolExample {

  private static final String ROOT_SG1_D1 = "root.sg1.d1";
  /**
   * set truststore.jks path only when iotdb_ssl_enable is true.
   * if modify iotdb_ssl_enable to false, modify IoTDB client's iotdb_ssl_enable="false" in iotdb-client.env,
   * iotdb-client.env file path: /opt/client/IoTDB/iotdb/conf
   */
  private static final String IOTDB_SSL_ENABLE = "true";

  private static SessionPool pool;
  private static ExecutorService service;

  public static void main(String[] args)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException, TTransportException {
    // set iotdb_ssl_enable
    System.setProperty("iotdb_ssl_enable", IOTDB_SSL_ENABLE);
    if ("true".equals(IOTDB_SSL_ENABLE)) {
      // set truststore.jks path
      System.setProperty("iotdb_ssl_truststore", "truststore文件路径");
    }

    pool = new SessionPool("127.0.0.1", 22260, "root", "root", 3);
    service = Executors.newFixedThreadPool(10);

    insertRecord();
    insertTablet();
    insertTablets();
    queryByRowRecord();
    queryByIterator();
    Thread.sleep(1000);
    pool.close();
    service.shutdown();
  }

  // more insert example, see SessionExample.java
  private static void insertRecord()
          throws StatementExecutionException, IoTDBConnectionException, TTransportException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 10; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      pool.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>Users need to control the count of Tablet and write a batch when it reaches the maxBatchSize
   */
  private static void insertTablet() throws IoTDBConnectionException, StatementExecutionException {
    /*
     * A Tablet example:
     *      device1
     * time s1, s2, s3
     * 1,   1,  1,  1
     * 2,   2,  2,  2
     * 3,   3,  3,  3
     */
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s3", TSDataType.INT64));

    Tablet tablet = new Tablet(ROOT_SG1_D1, schemaList, 100);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();

    for (long row = 0; row < 100; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int s = 0; s < 3; s++) {
        long value = new Random().nextLong();
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIndex, value);
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        pool.insertTablet(tablet, true);
        tablet.reset();
      }
      timestamp++;
    }

    if (tablet.rowSize != 0) {
      pool.insertTablet(tablet);
      tablet.reset();
    }

    // Method 2 to add tablet data
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;

    for (long time = 0; time < 100; time++) {
      int row = tablet.rowSize++;
      timestamps[row] = time;
      for (int i = 0; i < 3; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = i;
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        pool.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      pool.insertTablet(tablet);
      tablet.reset();
    }
  }

  private static void insertTablets() throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<MeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new MeasurementSchema("s1", TSDataType.INT64));
    schemaList.add(new MeasurementSchema("s2", TSDataType.INT64));

    Tablet tablet1 = new Tablet(ROOT_SG1_D1, schemaList, 100);
    Tablet tablet2 = new Tablet("root.sg1.d2", schemaList, 100);

    Map<String, Tablet> tabletMap = new HashMap<>();
    tabletMap.put(ROOT_SG1_D1, tablet1);
    tabletMap.put("root.sg1.d2", tablet2);

    // Method 1 to add tablet data
    long timestamp = System.currentTimeMillis();
    for (long row = 0; row < 100; row++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      tablet1.addTimestamp(row1, timestamp);
      tablet2.addTimestamp(row2, timestamp);

      for (int i = 0; i < 2; i++) {
        long value = new Random().nextLong();
        tablet1.addValue(schemaList.get(i).getMeasurementId(), row1, value);
        tablet2.addValue(schemaList.get(i).getMeasurementId(), row2, value);
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        pool.insertTablets(tabletMap, true);
        tablet1.reset();
        tablet2.reset();
      }
      timestamp++;
    }

    if (tablet1.rowSize != 0) {
      pool.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
    }

    // Method 2 to add tablet data
    long[] timestamps1 = tablet1.timestamps;
    Object[] values1 = tablet1.values;
    long[] timestamps2 = tablet2.timestamps;
    Object[] values2 = tablet2.values;

    for (long time = 0; time < 100; time++) {
      int row1 = tablet1.rowSize++;
      int row2 = tablet2.rowSize++;
      timestamps1[row1] = time;
      timestamps2[row2] = time;
      for (int i = 0; i < 2; i++) {
        long[] sensor1 = (long[]) values1[i];
        sensor1[row1] = i;
        long[] sensor2 = (long[]) values2[i];
        sensor2[row2] = i;
      }
      if (tablet1.rowSize == tablet1.getMaxRowNumber()) {
        pool.insertTablets(tabletMap, true);

        tablet1.reset();
        tablet2.reset();
      }
    }

    if (tablet1.rowSize != 0) {
      pool.insertTablets(tabletMap, true);
      tablet1.reset();
      tablet2.reset();
    }
  }

  private static void queryByRowRecord() {
    for (int i = 0; i < 1; i++) {
      service.submit(
          () -> {
            SessionDataSetWrapper wrapper = null;
            try {
              wrapper = pool.executeQueryStatement("select * from root.sg1.d1");
              System.out.println(wrapper.getColumnNames());
              System.out.println(wrapper.getColumnTypes());
              while (wrapper.hasNext()) {
                System.out.println(wrapper.next());
              }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              e.printStackTrace();
            } finally {
              // remember to close data set finally!
              pool.closeResultSet(wrapper);
            }
          });
    }
  }

  private static void queryByIterator() {
    for (int i = 0; i < 1; i++) {
      service.submit(
          () -> {
            SessionDataSetWrapper wrapper = null;
            try {
              wrapper = pool.executeQueryStatement("select * from root.sg1.d1");
              // get DataIterator like JDBC
              DataIterator dataIterator = wrapper.iterator();
              System.out.println(wrapper.getColumnNames());
              System.out.println(wrapper.getColumnTypes());
              while (dataIterator.next()) {
                StringBuilder builder = new StringBuilder();
                for (String columnName : wrapper.getColumnNames()) {
                  builder.append(dataIterator.getString(columnName) + " ");
                }
                System.out.println(builder);
              }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              e.printStackTrace();
            } finally {
              // remember to close data set finally!
              pool.closeResultSet(wrapper);
            }
          });
    }
  }
}
