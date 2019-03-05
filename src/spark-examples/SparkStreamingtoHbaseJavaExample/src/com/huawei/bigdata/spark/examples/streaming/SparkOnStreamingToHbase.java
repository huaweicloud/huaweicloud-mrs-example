package com.huawei.bigdata.spark.examples.streaming;


import com.huawei.hadoop.security.LoginUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * run streaming task and select table1 data from hbase, then update to table1
 */
public class SparkOnStreamingToHbase {
    private static final String TABLE_NAME = "table1";
    private static final String COLUMN_FAMILY = "cf";
    private static final String QUALIFIER = "cid";

    private static class Person {
        public Person(String id, float fee) {
            this.id = id;
            this.fee = fee;
        }

        String id;
        float fee;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            printUsage();
        }

        Configuration hadoopConf = new Configuration();
        if ("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))) {
            //security mode

            final String userPrincipal = "sparkuser";
            final String USER_KEYTAB_FILE = "user.keytab";
            String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            String krbFile = filePath + "krb5.conf";
            String userKeyTableFile = filePath + USER_KEYTAB_FILE;

            LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);
        }

        String checkPointDir = args[0];
        String topics = args[1];
        String brokers = args[2];

        final Duration batchDuration = Durations.seconds(5);
        SparkConf sparkConf = new SparkConf().setAppName("SparkOnStreamingToHbase");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, batchDuration);

        // set CheckPoint dir
        if (!"nocp".equals(checkPointDir)) {
            jssc.checkpoint(checkPointDir);
        }

        final String columnFamily = "cf";
        HashMap<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        String[] topicArr = topics.split(",");
        Set<String> topicSet = new HashSet<>(Arrays.asList(topicArr));

        // Create direct kafka stream with brokers and topics
        // Receive data from the Kafka and generate the corresponding DStream
        JavaDStream<String> lines = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicSet, kafkaParams)
        ).map(record -> String.valueOf(record.value()));

        lines.foreachRDD(rdd -> {
                    rdd.mapToPair(line -> new Tuple2<>(line.split(",")[0], Float.valueOf(line.split(",")[1])))
                            .reduceByKey((f1, f2) -> f1 + f2)
                            .map(pair -> new Person(pair._1, pair._2))
                            .foreachPartition(SparkOnStreamingToHbase::hBaseWriter);
                }
        );

        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * do write hbase in executor
     *
     * @param iterator message
     */
    private static void hBaseWriter(Iterator<Person> iterator) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = null;
        Table table = null;

        try {
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf(TABLE_NAME));

            List<Put> putList = new ArrayList<Put>();
            while (iterator.hasNext()) {
                Person person = iterator.next();
                byte[] rowKey = person.id.getBytes();
                Get get = new Get(rowKey);
                Result result = table.get(get);
                float oldValue = Float.valueOf(Bytes.toString(result.getValue(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes())));
                float newValue = oldValue + person.fee;
                Put put = new Put(rowKey);
                put.addColumn(COLUMN_FAMILY.getBytes(), QUALIFIER.getBytes(), Bytes.toBytes(String.valueOf(newValue)));
                putList.add(put);
            }

            if (putList.size() > 0) {
                table.put(putList);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    // Close the HBase connection.
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void printUsage() {
        System.out.println("Usage: SparkOnStreamingToHbase <checkPointDir> <topic> <brokerList>");
        System.exit(1);
    }
}
