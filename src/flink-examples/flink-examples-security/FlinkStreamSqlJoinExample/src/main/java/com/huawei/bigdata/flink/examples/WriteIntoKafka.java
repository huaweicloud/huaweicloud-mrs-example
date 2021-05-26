package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Random;

public class WriteIntoKafka {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka"
                        + " /opt/test.jar --topic topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21005");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka /opt/test.jar --topic"
                    + " topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21007 --security.protocol SASL_PLAINTEXT"
                    + " --sasl.kerberos.service.name kafka");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka /opt/test.jar --topic"
                    + " topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21008 --security.protocol SSL"
                    + " --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.WriteIntoKafka /opt/test.jar --topic"
                    + " topic-test -bootstrap.servers xxx.xxx.xxx.xxx:21009 --security.protocol SASL_SSL"
                    + " --sasl.kerberos.service.name kafka --ssl.truststore.location /home/truststore.jks"
                    + " --ssl.truststore.password huawei");
        System.out.println(
                "******************************************************************************************");
        System.out.println("<topic> is the kafka topic name");
        System.out.println("<bootstrap.servers> is the ip:port list of brokers");
        System.out.println(
                "******************************************************************************************");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        ParameterTool paraTool = ParameterTool.fromArgs(args);

        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());
        FlinkKafkaProducer<String> producer =
                new FlinkKafkaProducer<>(paraTool.get("topic"), new SimpleStringSchema(), paraTool.getProperties());

        producer.setWriteTimestampToKafka(true);

        messageStream.addSink(producer);
        env.execute();
    }

    public static class SimpleStringGenerator implements SourceFunction<String> {
        static final String[] NAME = {"Carry", "Alen", "Mike", "Ian", "John", "Kobe", "James"};
        static final String[] SEX = {"MALE", "FEMALE"};
        static final int COUNT = NAME.length;

        boolean running = true;
        Random rand = new Random(47);

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                int i = rand.nextInt(COUNT);
                int age = rand.nextInt(70);
                String sexy = SEX[rand.nextInt(2)];
                ctx.collect(NAME[i] + "," + age + "," + sexy);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
