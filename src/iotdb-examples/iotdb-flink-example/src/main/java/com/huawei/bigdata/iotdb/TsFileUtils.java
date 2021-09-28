/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utils used to prepare source TsFiles for the examples.
 *
 * @since 2021-07-28
 */
public class TsFileUtils {
  private static final Logger logger = LoggerFactory.getLogger(TsFileUtils.class);
  private static final String DEFAULT_TEMPLATE = "template";

  private TsFileUtils() {}

  public static void writeTsFile(String path) {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      Files.delete(f.toPath());
      Schema schema = new Schema();
      schema.extendTemplate(
          DEFAULT_TEMPLATE, new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
      schema.extendTemplate(
          DEFAULT_TEMPLATE,
          new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
      schema.extendTemplate(
          DEFAULT_TEMPLATE,
          new MeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));

      try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {

        // construct TSRecord
        for (int i = 0; i < 100; i++) {
          TSRecord tsRecord = new TSRecord(i, "device_" + (i % 4));
          DataPoint dPoint1 = new LongDataPoint("sensor_1", i);
          DataPoint dPoint2 = new LongDataPoint("sensor_2", i);
          DataPoint dPoint3 = new LongDataPoint("sensor_3", i);
          tsRecord.addTuple(dPoint1);
          tsRecord.addTuple(dPoint2);
          tsRecord.addTuple(dPoint3);

          // write TSRecord
          tsFileWriter.write(tsRecord);
        }
      }

    } catch (Exception e) {
      logger.error("Write {} failed. ", path, e);
    }
  }

  public static String[] readTsFile(String tsFilePath, List<Path> paths) throws IOException {
    QueryExpression expression = QueryExpression.create(paths, null);
    TsFileSequenceReader reader = new TsFileSequenceReader(tsFilePath);
    try (ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader)) {
      QueryDataSet queryDataSet = readTsFile.query(expression);
      List<String> result = new ArrayList<>();
      while (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        String row =
            rowRecord.getFields().stream()
                .map(f -> f == null ? "null" : f.getStringValue())
                .collect(Collectors.joining(","));
        result.add(rowRecord.getTimestamp() + "," + row);
      }
      return result.toArray(new String[0]);
    }
  }
}
