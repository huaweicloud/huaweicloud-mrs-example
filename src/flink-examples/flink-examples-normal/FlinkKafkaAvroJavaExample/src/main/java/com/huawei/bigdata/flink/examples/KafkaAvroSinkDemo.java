package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import com.huawei.mrs.flink.pojo.AvroData;

public class KafkaAvroSinkDemo {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaAvroSinkDemo.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Start Flink Streaming Source Java Demo for avro test.");
        ParameterTool params = ParameterTool.fromArgs(args);
        LOG.info("Params: " + params.toString());
        String bootstrapServers;
        String offsetPolicy;
        String group;
        String topic;
        bootstrapServers = params.get("bootstrap.servers", "xx.xx.xx.xx:xx");
        offsetPolicy = params.get("offset.policy", "latest");
        topic = params.get("topic", "test_topic");
        group = params.get("group.id", "test_group");

        try {
            StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

            Properties sourceProperties = new Properties();
            sourceProperties.setProperty("bootstrap.servers", bootstrapServers);
            sourceProperties.setProperty("group.id", group);
            sourceProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetPolicy);

            DataStream<ConsumerRecord<byte[], byte[]>> stream = streamEnv.addSource(
                new FlinkKafkaConsumer<>(topic,
                    new ConsumerRecordDeserializationSchema(),
                    sourceProperties)).setParallelism(2)
//                    .disableChaining()
                    .rebalance();

            stream.process(new JsonDeserializerProcessFunction<>(AvroData.class)).setParallelism(3);

            stream.print();

            streamEnv.execute("Test Avro Program");
        } catch (Exception e) {
            LOG.error("The Exception is " + e.getMessage(), e);
        }
    }
}

