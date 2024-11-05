/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Consumes messages from one or more topics in Kafka.
 * <filePath> is for text file to read data, use comma to separate
 * <windowTime> is the width of the window, time as minutes
 *
 * @since 8.0.2
 */
public class FlinkStreamJavaExample {
    public static void main(String[] args) throws Exception {
        // print comment for command to use run flink
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.FlinkStreamJavaExample"
                        + " /opt/test.jar --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2");
        System.out.println(
                "******************************************************************************************");
        System.out.println("<filePath> is for text file to read data, use comma to separate");
        System.out.println("<windowTime> is the width of the window, time as minutes");
        System.out.println(
                "******************************************************************************************");

        // Checking input parameters
        final String[] filePaths =
                ParameterTool.fromArgs(args).get("filePath", "/opt/log1.txt,/opt/log2.txt").split(",");
        assert filePaths.length > 0;

        // windowTime is for the width of window, as the data read from filePaths quickly
        // , 2 minutes is enough to read all data
        final int windowTime = ParameterTool.fromArgs(args).getInt("windowTime", 2);

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(200);
        env.setParallelism(1);

        // get input data
        DataStream<String> unionStream = env.readTextFile(filePaths[0]);
        if (filePaths.length > 1) {
            for (int i = 1; i < filePaths.length; i++) {
                unionStream = unionStream.union(env.readTextFile(filePaths[i]));
            }
        }

        // data transform
        unionStream
                .map(
                        new MapFunction<String, UserRecord>() {
                            @Override
                            public UserRecord map(String value) throws Exception {
                                return getRecord(value);
                            }
                        })
                .assignTimestampsAndWatermarks(new Record2TimestampExtractor())
                .filter(
                        new FilterFunction<UserRecord>() {
                            @Override
                            public boolean filter(UserRecord value) throws Exception {
                                return value.gender.equals("female");
                            }
                        })
                .keyBy(new UserRecordSelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(windowTime)))
                .reduce(
                        new ReduceFunction<UserRecord>() {
                            @Override
                            public UserRecord reduce(UserRecord value1, UserRecord value2) throws Exception {
                                value1.shoppingTime += value2.shoppingTime;
                                return value1;
                            }
                        })
                .filter(
                        new FilterFunction<UserRecord>() {
                            @Override
                            public boolean filter(UserRecord value) throws Exception {
                                return value.shoppingTime > 120;
                            }
                        })
                .print();

        // execute program
        env.execute("FemaleInfoCollectionPrint java");
    }

    private static class UserRecordSelector implements KeySelector<UserRecord, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(UserRecord value) throws Exception {
            return Tuple2.of(value.name, value.gender);
        }
    }

    private static UserRecord getRecord(String line) {
        String[] elems = line.split(",");
        assert elems.length == 3;
        return new UserRecord(elems[0], elems[1], Integer.parseInt(elems[2]));
    }

    /**
     * @since 8.0.2
     */
    public static class UserRecord {
        private String name;
        private String gender;
        private int shoppingTime;

        public UserRecord(String nm, String sx, int st) {
            name = nm;
            gender = sx;
            shoppingTime = st;
        }

        /**
         * @return string
         */
        public String toString() {
            return "name: " + name + "  gender: " + gender + "  shoppingTime: " + shoppingTime;
        }
    }

    // class to set watermark and timestamp
    private static class Record2TimestampExtractor implements AssignerWithPunctuatedWatermarks<UserRecord> {
        // add tag in the data of datastream elements
        @Override
        public long extractTimestamp(UserRecord element, long previousTimestamp) {
            return System.currentTimeMillis();
        }

        // give the watermark to trigger the window to execute, and use the value to check if the window elements is
        // ready
        @Override
        public Watermark checkAndGetNextWatermark(UserRecord element, long extractedTimestamp) {
            return new Watermark(extractedTimestamp - 1);
        }
    }
}
