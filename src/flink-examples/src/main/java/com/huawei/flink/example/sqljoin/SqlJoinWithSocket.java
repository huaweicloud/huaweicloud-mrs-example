package com.huawei.flink.example.sqljoin;

import org.apache.calcite.interpreter.Row;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class SqlJoinWithSocket {

    public static void main(String[] args) throws Exception{

        final String hostname;

        final int port;

        System.out.println("use command as: ");

        System.out.println("flink run --class com.huawei.flink.example.sqljoin.SqlJoinWithSocket" +
                " /opt/test.jar --topic topic-test -bootstrap.servers xxxx.xxx.xxx.xxx:9092 --hostname xxx.xxx.xxx.xxx --port xxx");

        System.out.println("flink run --class com.huawei.flink.example.sqljoin.SqlJoinWithSocket" +
                " /opt/test.jar --topic topic-test -bootstrap.servers xxxx.xxx.xxx.xxx:21007 --security.protocol SASL_PLAINTEXT --sasl.kerberos.service.name kafka"
                + "--hostname xxx.xxx.xxx.xxx --port xxx");

        System.out.println("flink run --class com.huawei.flink.example.sqljoin.SqlJoinWithSocket" +
                " /opt/test.jar --topic topic-test -bootstrap.servers xxxx.xxx.xxx.xxx:21008 --security.protocol SSL --ssl.truststore.location /home/truststore.jks "
                + "--ssl.truststore.password huawei --hostname xxx.xxx.xxx.xxx --port xxx");

        System.out.println("******************************************************************************************");
        System.out.println("<topic> is the kafka topic name");
        System.out.println("<bootstrap.servers> is the ip:port list of brokers");
        System.out.println("******************************************************************************************");

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);

            hostname = params.has("hostname") ? params.get("hostname") : "localhost";

            port = params.getInt("port");

        } catch (Exception e) {
            System.err.println("No port specified. Please run 'FlinkStreamSqlJoinExample " +
                    "--hostname <hostname> --port <port>', where hostname (localhost by default) " +
                    "and port is the address of the text server");

            System.err.println("To start a simple text server, run 'netcat -l -p <port>' and " +
                    "type the input text into the command line");

            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        //基于EventTime进行处理
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.setParallelism(1);

        ParameterTool paraTool = ParameterTool.fromArgs(args);

        //Stream1，从Kafka中读取数据
        DataStream<Tuple3<String, String, String>> kafkaStream = env.addSource(new FlinkKafkaConsumer<>(paraTool.get("topic"),
                new SimpleStringSchema(), paraTool.getProperties()))
                .map(new MapFunction<String, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(String s) throws Exception
                    {
                        String[] word = s.split(",");

                        return new Tuple3<>(word[0], word[1], word[2]);
                    }
                });

        //将Stream1注册为Table1
        tableEnv.registerDataStream("Table1", kafkaStream, "name, age, sexy, proctime.proctime");

        //Stream2，从Socket中读取数据
        DataStream<Tuple2<String, String>> socketStream = env.socketTextStream(hostname, port, "\n")
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception
                    {
                        String[] words = s.split("\\s");
                        if (words.length < 2) {
                            return new Tuple2<>();
                        }

                        return new Tuple2<>(words[0], words[1]);
                    }
                });

        //将Stream2注册为Table2
        tableEnv.registerDataStream("Table2", socketStream, "name, job, proctime.proctime");

        //执行SQL Join进行联合查询
        Table result = tableEnv.sqlQuery("SELECT t1.name, t1.age, t1.sexy, t2.job, t2.proctime as shiptime\n" +
                "FROM Table1 AS t1\n" +
                "JOIN Table2 AS t2\n" +
                "ON t1.name = t2.name\n" +
                "AND t1.proctime BETWEEN t2.proctime - INTERVAL '1' SECOND AND t2.proctime + INTERVAL '1' SECOND");

        //将查询结果转换为Stream，并打印输出
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
