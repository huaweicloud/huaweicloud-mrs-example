package com.huawei.flink.example.sqljoin;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Random;

public class WriteIntoKafka4SQLJoin {

    public static void main(String[] args) throws Exception {

        // æå°åºæ§è¡flink runçåèå½ä»¤
        System.out.println("use command as: ");

        System.out.println("./bin/flink run --class com.huawei.flink.example.sqljoin.WriteIntoKafka4SQLJoin" +

                " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21005");

        System.out.println("./bin/flink run --class com.huawei.flink.example.sqljoin.WriteIntoKafka4SQLJoin" +

                " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21007 --security.protocol SASL_PLAINTEXT --sasl.kerberos.service.name kafka");

        System.out.println("./bin/flink run --class com.huawei.flink.example.sqljoin.WriteIntoKafka4SQLJoin" +

                " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21008 --security.protocol SSL --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");

        System.out.println("./bin/flink run --class com.huawei.flink.example.sqljoin.WriteIntoKafka4SQLJoin" +

                " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21009 --security.protocol SASL_SSL --sasl.kerberos.service.name kafka --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");

        System.out.println("******************************************************************************************");

        System.out.println("<topic> is the kafka topic name");

        System.out.println("<bootstrap.servers> is the ip:port list of brokers");

        System.out.println("******************************************************************************************");

        // æé æ§è¡ç¯å¢
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // è®¾ç½®å¹¶ååº¦
        env.setParallelism(1);
        // è§£æè¿è¡åæ°
        ParameterTool paraTool = ParameterTool.fromArgs(args);

        // æé æµå¾ï¼å°èªå®ä¹Sourceçæçæ°æ®åå¥Kafka
        DataStream<String> messageStream = env.addSource(new SimpleStringGenerator());

        FlinkKafkaProducer producer = new FlinkKafkaProducer<>(paraTool.get("topic"), new SimpleStringSchema(), paraTool.getProperties());

        messageStream.addSink(producer);

        // è°ç¨executeè§¦åæ§è¡
        env.execute();
    }

    // èªå®ä¹Sourceï¼æ¯é1sæç»­äº§çæ¶æ¯
    public static class SimpleStringGenerator implements SourceFunction<String> {
        static final String[] NAME = {"Carry", "Alen", "Mike", "Ian", "John", "Kobe", "James"};

        static final String[] SEX = {"MALE", "FEMALE"};

        static final int COUNT = NAME.length;

        boolean running = true;

        Random rand = new Random(47);

        @Override
        //randéæºäº§çåå­ï¼æ§å«ï¼å¹´é¾çç»åä¿¡æ¯
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
