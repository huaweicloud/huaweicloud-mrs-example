/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.HashMap;
import java.util.Map;

/**
 * @since 8.2.1
 */

public class ReadFromHudi {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run -m yarn-cluster --class com.huawei.bigdata.flink.examples.ReadFromHudi"
                        + " /opt/test.jar --hudiTableName hudiSourceTable --hudiPath hdfs://hacluster/tmp/flinkHudi/hudiTable"
                        + " --read.start-commit 20221206111532"
        );
        System.out.println(
                "******************************************************************************************");
        System.out.println("<hudiTableName> is the hoodie table name. (Default value is hudiSourceTable)");
        System.out.println("<hudiPath> Base path for the target hoodie table. (Default value is hdfs://hacluster/tmp/flinkHudi/hudiTable)");
        System.out.println("<read.start-commit> Start commit instant for reading, the commit time format should be 'yyyyMMddHHmmss'. (Default value is earliest)");
        System.out.println(
                "******************************************************************************************");

        ParameterTool paraTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String basePath = paraTool.get("hudiPath", "hdfs://hacluster/tmp/flinkHudi/hudiTable");
        String targetTable = paraTool.get("hudiTableName", "hudiSourceTable");
        String startCommit = paraTool.get(FlinkOptions.READ_START_COMMIT.key(), FlinkOptions.START_COMMIT_EARLIEST);
        Map<String, String> options = new HashMap();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.READ_AS_STREAMING.key(), "true"); // this option enable the streaming read
        options.put(FlinkOptions.READ_START_COMMIT.key(), startCommit); // specifies the start commit instant time
        HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
                .column("uuid VARCHAR(20)")
                .column("name VARCHAR(10)")
                .column("age INT")
                .column("ts TIMESTAMP(3)")
                .column("p VARCHAR(20)")
                .pk("uuid")
                .partition("p")
                .options(options);

        DataStream<RowData> rowDataDataStream = builder.source(env);
        rowDataDataStream.map(new MapFunction<RowData, String>() {
            @Override
            public String map(RowData rowData) throws Exception {
                StringBuilder sb = new StringBuilder();
                sb.append("{");
                sb.append("\"uuid\":\"").append(rowData.getString(0)).append("\",");
                sb.append("\"name\":\"").append(rowData.getString(1)).append("\",");
                sb.append("\"age\":").append(rowData.getInt(2)).append(",");
                sb.append("\"ts\":\"").append(rowData.getTimestamp(3, 0)).append("\",");
                sb.append("\"p\":\"").append(rowData.getString(4)).append("\"");
                sb.append("}");
                return sb.toString();
            }
        }).print();
        env.execute("Hudi_Source");
    }
}