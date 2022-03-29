/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021. All rights reserved.
 */

package com.huawei.bigdata.iotdb;

import org.apache.iotdb.flink.tsfile.RowRowRecordParser;
import org.apache.iotdb.flink.tsfile.TsFileInputFormat;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The example of reading TsFile via Flink DataSet API.
 *
 * @since 2021-07-28
 */
public class FlinkTsFileBatchSource {
  public static void main(String[] args) throws Exception {
    String path = "test.tsfile";
    // create a new tsfile
    TsFileUtils.writeTsFile(path);
    // after the program exit, the tsfile will be deleted, comment out if you want to keep the tsfile.
    new File(path).deleteOnExit();
    String[] filedNames = {
      QueryConstant.RESERVED_TIME,
      "device_1.sensor_1",
      "device_1.sensor_2",
      "device_1.sensor_3",
      "device_2.sensor_1",
      "device_2.sensor_2",
      "device_2.sensor_3"
    };
    TypeInformation[] typeInformations =
        new TypeInformation[] {
          Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG, Types.LONG
        };
    List<Path> paths =
        Arrays.stream(filedNames)
            .filter(s -> !s.equals(QueryConstant.RESERVED_TIME))
            .map(s -> new Path(s, true))
            .collect(Collectors.toList());
    RowTypeInfo rowTypeInfo = new RowTypeInfo(typeInformations, filedNames);
    QueryExpression queryExpression = QueryExpression.create(paths, null);
    RowRowRecordParser parser =
        RowRowRecordParser.create(rowTypeInfo, queryExpression.getSelectedSeries());
    TsFileInputFormat<Row> inputFormat = new TsFileInputFormat<>(queryExpression, parser);
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    inputFormat.setFilePath(path);
    DataSet<Row> source = env.createInput(inputFormat);
    List<String> result = source.map(Row::toString).collect();
    for (String s : result) {
      System.out.println(s);
    }
  }
}
