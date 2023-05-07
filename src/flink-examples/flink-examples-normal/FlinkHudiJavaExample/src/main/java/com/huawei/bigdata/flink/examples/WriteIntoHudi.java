/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 8.2.1
 */

public class WriteIntoHudi {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run -m yarn-cluster --class com.huawei.bigdata.flink.examples.WriteIntoHudi"
                        + " /opt/test.jar --hudiTableName hudiSinkTable --hudiPath hdfs://hacluster/tmp/flinkHudi/hudiTable");
        System.out.println(
                "******************************************************************************************");
        System.out.println("<hudiTableName> is the hudi table name. (Default value is hudiSinkTable)");
        System.out.println("<hudiPath> Base path for the target hoodie table. (Default value is hdfs://hacluster/tmp/flinkHudi/hudiTable)");
        System.out.println(
                "******************************************************************************************");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(10000);
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        DataStream<RowData> stringDataStreamSource = env.addSource(new SimpleStringGenerator())
                .map(new MapFunction<Tuple5<String, String, Integer, String, String>, RowData>() {
                    @Override
                    public RowData map(Tuple5<String, String, Integer, String, String> tuple5) throws Exception {
                        GenericRowData rowData = new GenericRowData(5);
                        rowData.setField(0, StringData.fromString(tuple5.f0));
                        rowData.setField(1, StringData.fromString(tuple5.f1));
                        rowData.setField(2, tuple5.f2);
                        rowData.setField(3, TimestampData.fromTimestamp(Timestamp.valueOf(tuple5.f3)));
                        rowData.setField(4, StringData.fromString(tuple5.f4));
                        return rowData;
                    }
                });
        String basePath = paraTool.get("hudiPath", "hdfs://hacluster/tmp/flinkHudi/hudiTable");
        String targetTable = paraTool.get("hudiTableName", "hudiSinkTable");
        Map<String, String> options = new HashMap<>();
        options.put(FlinkOptions.PATH.key(), basePath);
        options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
        options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
        options.put(FlinkOptions.INDEX_BOOTSTRAP_ENABLED.key(), "true");
        HoodiePipeline.Builder builder = HoodiePipeline.builder(targetTable)
                .column("uuid VARCHAR(20)")
                .column("name VARCHAR(10)")
                .column("age INT")
                .column("ts TIMESTAMP(3)")
                .column("p VARCHAR(20)")
                .pk("uuid")
                .partition("p")
                .options(options);
        builder.sink(stringDataStreamSource, false); // The second parameter indicating whether the input data stream is bounded
        env.execute("Hudi_Sink");
    }

    /**
     * @since 8.2.1
     */
    public static class SimpleStringGenerator implements SourceFunction<Tuple5<String, String, Integer, String, String>> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        Integer i = 0;

        @Override
        public void run(SourceContext<Tuple5<String, String, Integer, String, String>> ctx) throws Exception {
            while (running) {
                i++;
                String uuid = "uuid" + i;
                String name = "name" + i;
                Integer age = new Integer(i);
                String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                String p = "par" + i % 5;
                Tuple5<String, String, Integer, String, String> tuple5 = Tuple5.of(uuid, name, age, ts, p);
                ctx.collect(tuple5);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
