package com.huawei.flink.example.kafka;

//import com.huawei.flink.example.common.SimpleStringGenerator;
import com.huawei.flink.example.common.SimpleStringGenerator;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;


public class WriteIntoKafka {

    public static void main(String[] args) throws Exception
    {
        // 打印出执行flink run的参考命令

        System.out.println("use command as: ");

        System.out.println("./bin/flink run --class com.huawei.flink.example.kafka.WriteIntoKafka" +

                " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21005");

        System.out.println("./bin/flink run --class com.huawei.flink.example.kafka.WriteIntoKafka" +

                " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21007 --security.protocol SASL_PLAINTEXT --sasl.kerberos.service.name kafka");

        System.out.println("./bin/flink run --class com.huawei.flink.example.kafka.WriteIntoKafka" +

                " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21008 --security.protocol SSL --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");

        System.out.println("./bin/flink run --class com.huawei.flink.example.kafka.WriteIntoKafka" +

                " /opt/test.jar --topic topic-test -bootstrap.servers 10.91.8.218:21009 --security.protocol SASL_SSL --sasl.kerberos.service.name kafka --ssl.truststore.location /home/truststore.jks --ssl.truststore.password huawei");

        System.out.println("******************************************************************************************");

        System.out.println("<topic> is the kafka topic name");

        System.out.println("<bootstrap.servers> is the ip:port list of brokers");

        System.out.println("******************************************************************************************");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        DataStream<String> dataStream = env.addSource(new SimpleStringGenerator());

        dataStream.addSink(new FlinkKafkaProducer<String>(parameterTool.get("topic"),
                new SimpleStringSchema(), parameterTool.getProperties()));

        env.execute();

    }

}


