/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * kafka example启动类
 * 
 * @since 2019/9/30
 */
public class FemaleInfoCollectionFromKafka {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.FemaleInfoCollectionFromKafka"
                        + " /opt/test.jar --windowTime 2 --topic topic-test --bootstrap.servers xxx.xxx.xxx.xxx:21005");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.FemaleInfoCollectionFromKafka /opt/test.jar"
                    + " --windowTime 2 --topic topic-test --bootstrap.servers xxx.xxx.xxx.xxx:21007"
                    + " --security.protocol SASL_PLAINTEXT --sasl.kerberos.service.name kafka");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.FemaleInfoCollectionFromKafka /opt/test.jar"
                    + " --windowTime 2 --topic topic-test --bootstrap.servers xxx.xxx.xxx.xxx:21008"
                    + " --security.protocol SSL --ssl.truststore.location /home/truststore.jks"
                    + " --ssl.truststore.password huawei");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.FemaleInfoCollectionFromKafka /opt/test.jar"
                    + " --windowTime 2 --topic topic-test --bootstrap.servers xxx.xxx.xxx.xxx:21009"
                    + " --security.protocol SASL_SSL --sasl.kerberos.service.name kafka --ssl.truststore.location"
                    + " /home/truststore.jks --ssl.truststore.password huawei");
        System.out.println(
                "******************************************************************************************");
        System.out.println("<windowTime> is the width of the window, time as minutes");
        System.out.println("<topic> is the kafka topic name");
        System.out.println("<bootstrap.servers> is the ip:port list of brokers");
        System.out.println(
                "******************************************************************************************");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        final Integer windowTime = paraTool.getInt("windowTime", 2);

        DataStream<String> messageStream =
                env.addSource(
                        new FlinkKafkaConsumer<>(
                                paraTool.get("topic"), new SimpleStringSchema(), paraTool.getProperties()));
        messageStream
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
                                return value.sexy.equals("female");
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
        env.execute();
    }

    private static class UserRecordSelector implements KeySelector<UserRecord, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(UserRecord value) throws Exception {
            return Tuple2.of(value.name, value.sexy);
        }
    }

    private static UserRecord getRecord(String line) {
        String[] elems = line.split(",");
        assert elems.length == 3;
        return new UserRecord(elems[0], elems[1], Integer.parseInt(elems[2]));
    }

    /**
     * 用户订单类
     * 
     * @since 2019/9/30
     */
    public static class UserRecord {
        private String name;
        private String sexy;
        private Integer shoppingTime;

        public UserRecord(String name, String sexy, Integer shoppingTime) {
            this.name = name;
            this.sexy = sexy;
            this.shoppingTime = shoppingTime;
        }

        @Override
        public String toString() {
            return "name: " + name + "  sexy: " + sexy + "  shoppingTime: " + shoppingTime.toString();
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
