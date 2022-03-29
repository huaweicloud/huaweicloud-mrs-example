package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FromKafkaToHBase {
    private static final Logger logger = LoggerFactory.getLogger(FromKafkaToHBase.class);
    private static final String topic = "flink_topic_test";

    public static Properties configKafka() {
        Properties props = new Properties();
        //kerberos集群根据需要进行添加修改
        //props.put("bootstrap.servers", "127.0.0.1:21007,127.0.0.2:21007");
        props.put("bootstrap.servers", "192.168.0.xxx:9092");
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "DemoConsumertest");
        // kerberos cluster needs
        // props.put("auto.offset.reset", "latest");
        // kerberos cluster needs
        // put("sasl.kerberos.service.name","kafka");
        // kerberos cluster needs
        // props.put("security.protocol","SASL_PLAINTEXT");
        // kerberos cluster needs
        // props.put("kerberos.domain.name","hadoop.xxxxxxx.com");
        return props;
    }

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        logger.info("begin read from kafka");
        DataStream<String> transction = env.addSource(
                new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), configKafka()));
        transction.writeUsingOutputFormat(new WriteToHBase());

        try {
            // run the job.
            env.execute("WriteToHBase");
        } catch (Exception e) {
            logger.error("read data failed from hbase, ", e);
        }
    }
}
