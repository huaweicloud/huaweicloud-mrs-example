package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadFromKafkaUseKafkaSource {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.ReadFromKafka"
                        + " /opt/test.jar --topic topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21005");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.ReadFromKafka /opt/test.jar --topic"
                        + " topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21007 --security.protocol SASL_PLAINTEXT"
                        + " --sasl.kerberos.service.name kafka");
        System.out.println(
                "******************************************************************************************");
        System.out.println("<topic> is the kafka topic name");
        System.out.println("<bootstrap.servers> is the ip:port list of brokers");
        System.out.println(
                "******************************************************************************************");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(paraTool.get("bootstrap.servers"))
                .setTopics(paraTool.get("topic"))
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(paraTool.getProperties())
                .build();

        DataStreamSource<String> messageStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        messageStream
                .rebalance()
                .map(
                        new MapFunction<String, String>() {
                            @Override
                            public String map(String s) throws Exception {
                                return "Flink says " + s + System.getProperty("line.separator");
                            }
                        })
                .print();
        env.execute();
    }
}
