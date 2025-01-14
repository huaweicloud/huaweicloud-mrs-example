package com.huawei.bigdata.flink.examples.cep;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * CEP job example. It generates data to kafka 'source' topic and reads data from this topic in
 * another job in single statement set.
 */
public class CEPExample {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.cep.CEPExample"
                        + " /opt/test.jar -bootstrap.servers xxx.xxx.xxx.xxx:21005");
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        String bootstrapServers = paraTool.get("bootstrap.servers");

        String sourceGroupId = "source-test";
        String sourceTopic = "source";

        String patternGroupId = "patterns-test";
        String patternTopic = "patterns";

        Configuration conf = new Configuration();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        TableEnvironment tEnv = StreamTableEnvironment.create(env);

        runJob(bootstrapServers, patternGroupId, patternTopic, sourceGroupId, sourceTopic, tEnv);
    }

    private static void runJob(
            String bootstrapServers,
            String patternGroupId,
            String patternTopic,
            String sourceGroupId,
            String sourceTopic,
            TableEnvironment tEnv)
            throws Exception {
        tEnv.executeSql(
                "CREATE TABLE datagen_source ("
                        + " id INT,"
                        + " num INT"
                        + ") WITH ("
                        + " 'connector' = 'datagen',"
                        + " 'number-of-rows' = '500000',"
                        + " 'rows-per-second' = '1000',"
                        + " 'fields.num.kind' = 'random',"
                        + " 'fields.num.min' = '0',"
                        + " 'fields.num.max' = '10'"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE kafka_sink ("
                        + "id INT,"
                        + "num INT"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '"
                        + sourceTopic
                        + "',"
                        + "'properties.bootstrap.servers' = '"
                        + bootstrapServers
                        + "',"
                        + "'format' = 'csv',"
                        + "'scan.startup.mode' = 'earliest-offset'"
                        + ")");

        String sourceSql =
                "CREATE TABLE kafka_source ("
                        + "  id INT,"
                        + "  num INT,"
                        + "  proc_time AS PROCTIME()"
                        + ") WITH ("
                        + " 'connector' = 'kafka',"
                        + " 'topic' = '"
                        + sourceTopic
                        + "',"
                        + " 'properties.bootstrap.servers' = '"
                        + bootstrapServers
                        + "',"
                        + " 'properties.group.id' = '"
                        + sourceGroupId
                        + "',"
                        + " 'scan.startup.mode' = 'latest-offset',"
                        + " 'format' = 'csv'"
                        + " )";
        tEnv.executeSql(sourceSql);

        String sinkSql =
                "CREATE TABLE sink (" + "  ids STRING," + "  num INT" + ") WITH (" + " 'connector' = 'print'" + ")";
        tEnv.executeSql(sinkSql);

        String jobSql =
                "INSERT INTO sink"
                        + " SELECT T.ids, T.num"
                        + " FROM kafka_source"
                        + " MATCH_RECOGNIZE_DYNAMIC ("
                        + " ORDER BY proc_time"
                        + " MEASURES"
                        + "   ids STRING,"
                        + "   num INT"
                        + " OPTIONS"
                        + "  'pattern.discoverer' = 'from-kafka-pattern-discoverer',"
                        + "  'coordinator.on-failure' = 'FAIL_JOB',"
                        + "  'coordinator.on-empty-processors' = 'FAIL_JOB',"
                        + "  'from-kafka-pattern-discoverer.bootstrap.servers' = '"
                        + bootstrapServers
                        + "',"
                        + "  'from-kafka-pattern-discoverer.group.id' = '"
                        + patternGroupId
                        + "',"
                        + "  'from-kafka-pattern-discoverer.topic' = '"
                        + patternTopic
                        + "'"
                        + ") AS T";
        tEnv.executeSql(
                        "EXECUTE STATEMENT SET\n"
                                + "BEGIN\n"
                                + " INSERT INTO kafka_sink SELECT * FROM datagen_source;\n"
                                + " "
                                + jobSql
                                + ";\n"
                                + "END;\n")
                .await();
    }
}
