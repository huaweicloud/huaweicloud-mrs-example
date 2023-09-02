/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.time.Duration;


public class WriteIntoOBS {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run -m yarn-cluster --class com.huawei.bigdata.flink.examples.WriteIntoOBS"
                        + " /opt/test.jar --obsPath obs://test_bucket/tmp/flinkobs/output");
        System.out.println(
                "******************************************************************************************");
        System.out.println("<obsPath> Base path for the obs path. (Default value is obs://test_bucket/tmp/flinkobs/output)");
        System.out.println(
                "******************************************************************************************");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(10000);
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        String obsPath = paraTool.get("obsPath", "obs://test_bucket/tmp/flinkobs/output");
        DataStreamSource<Tuple3<String, String, String>> source = env.addSource(new SimpleStringGenerator());
        SingleOutputStreamOperator<String> map = source.map(new MapFunction<Tuple3<String, String, String>, String>() {

            @Override
            public String map(Tuple3<String, String, String> tuple3) throws Exception {
                return tuple3.f0 + "|" + tuple3.f1 + "|" + tuple3.f2;
            }
        });
        FileSink<String> sink = FileSink
                .forRowFormat(new Path(obsPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withInactivityInterval(Duration.ofSeconds(10))
                                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                .build())
                .build();
        map.sinkTo(sink);
        env.execute("OBS_Sink");
    }


    public static class SimpleStringGenerator implements SourceFunction<Tuple3<String, String, String>> {
        boolean running = true;
        Integer i = 0;

        @Override
        public void run(SourceContext<Tuple3<String, String, String>> ctx) throws Exception {
            while (running) {
                i++;
                String uuid = "uuid" + i;
                String name = "name" + i;
                String info = "info" + i % 5;
                Tuple3<String, String,  String> tuple3 = Tuple3.of(uuid, name, info);
                ctx.collect(tuple3);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
