/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;


public class ReadFromOBS {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run -m yarn-cluster --class com.huawei.bigdata.flink.examples.ReadFromOBS"
                        + " /opt/test.jar --obsPath obs://test_bucket/tmp/flinkobs/input"
        );
        System.out.println(
                "******************************************************************************************");
        System.out.println("<obsPath> Base path for the obs path. (Default value is obs://test_bucket/tmp/flinkobs/input)");
        System.out.println(
                "******************************************************************************************");

        ParameterTool paraTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String obsPath = paraTool.get("obsPath", "obs://test_bucket/tmp/flinkobs/input");
        CsvReaderFormat<SomePojo> csvFormat = CsvReaderFormat.forPojo(SomePojo.class);
        FileSource<SomePojo> source = FileSource
                .forRecordStreamFormat(csvFormat, new Path(obsPath))
                .monitorContinuously(Duration.ofMillis(5))
                .build();
        env.fromSource(source, WatermarkStrategy.noWatermarks(),"obs-source")
                .map(
                        new MapFunction<SomePojo, String>() {
                            @Override
                            public String map(SomePojo somePojo) throws Exception {
                                return somePojo.toString();
                            }
                        }).print();
        env.execute("OBS_Source");
    }
}