package com.huawei.bigdata.spark.examples.streaming;

import com.huawei.bigdata.createTable.CreateIndex;
import com.huawei.bigdata.createTable.CreateTable;
import com.huawei.hadoop.security.LoginUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.time.LocalDate;
import java.util.*;

/**
 * consuming kafka topic detaildata and reportdata by spark streaming, and write data to hbase, elastic search and hdfs.
 */
public class SparkOnStreamingToESHdfsHBaseExample {
    private static Configuration conf = null;
    private static final String ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME = "Client";
    private static String krb5File = null;
    private static String userName = null;
    private static String userKeytabFile = null;
    private static RestHighLevelClient highLevelClient = null;
    private final static Log LOG = LogFactory.getLog(SparkOnStreamingToESHdfsHBaseExample.class.getName());
    private static String user_confdir = "/opt/sandbox/";

    public static void main(String[] args) throws Exception {
        try {
            init();
            login();
        } catch (IOException e) {
            LOG.error("Failed to login because ", e);
            return;
        }

        String DetailTable = "detail_" + LocalDate.now().getYear() + "_" + LocalDate.now().getMonthValue();
        String ReportTable = "report_" + LocalDate.now().getYear() + "_" + LocalDate.now().getMonthValue();
        String[] tables = {ReportTable, DetailTable};
        // 检查hbase表是否创建成功
        CreateTable c = new CreateTable();
        c.InitHBase(tables, conf);
        // 检查es
        highLevelClient = getRestClient();
        CreateIndex.createIndex(highLevelClient, tables);

        if (args.length < 3) {
            printUsage();
        }
        String topics = args[1];
        final String brokers = args[2];
        System.out.println("topics=" + topics);
        System.out.println("brokers=" + brokers);

        SparkConf sparkConf = new SparkConf().setAppName("SparkOnStreamingToESHdfsHbase_master");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(Integer.parseInt("10")));

        final SQLContext sqlContext = new SQLContext(jssc.ssc().sc());

        JavaHBaseContext hbaseContext = new JavaHBaseContext(jssc.sparkContext(), conf);

        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "testWriteHBaseEsHdfs");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        kafkaParams.put("enable.auto.commit", true);
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");
        kafkaParams.put("kerberos.domain.name", "hadoop.hadoop.com");

        String[] topicArr = topics.split(","); // detaildata,reportdata
        Set<String> topicSet = new HashSet<String>(Arrays.asList(topicArr));

        // Create direct kafka stream with brokers and topics
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topicSet, kafkaParams));

        JavaDStream<String> lines =
                stream.map(
                        (Function<ConsumerRecord<String, String>, String>) v1 -> {
                            System.out.println("-----------v1.value---------:" + v1.value());
                            return v1.value();
                        });

        JavaDStream<String> DetailRDD = lines.filter((Function<String, Boolean>) s -> {
            String[] data = s.split("\\|");
            System.out.println("===data length===" + data.length);
            // 用List长度来过滤
            if (data.length < 7) {
                System.out.println("-------------detail data------: " + s);
                return true;
            }
            return false;
        });

        DetailRDD.print();

        JavaDStream<String> ReportRDD = lines.filter((Function<String, Boolean>) s -> {
            String[] data = s.split("\\|");
            // 用List长度来过滤
            if (data.length > 7) {
                System.out.println("-------------report data------: " + s);
                return true;
            }
            return false;
        });

        ReportRDD.print();

        // 1. 写hbase
        hbaseContext.streamBulkPut(DetailRDD, TableName.valueOf(DetailTable), new DetailFunction());
        hbaseContext.streamBulkPut(ReportRDD, TableName.valueOf(ReportTable), new ReportFunction());

        // 2. 写parquet
        DetailRDD.foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> {
            JavaRDD<Row> javaRDD = stringJavaRDD.map((Function<String, Row>) s -> {
                System.out.println("---write parquet-------" + s);
                String[] data = s.split("\\|");
                return RowFactory.create(data[0], data[1], data[2], data[3], data[4], data[5]);
            });
            // Date,Gateway_Name,Enterprise_Code,Business_Code,Service_Code,Send_Direction
            StructType schema = new StructType().add("Date", "String")
                    .add("Gateway_Name", "String")
                    .add("Enterprise_Code", "String")
                    .add("Business_Code", "String")
                    .add("Service_Code", "String")
                    .add("Send_Direction", "String");
            Dataset<Row> dataset = sqlContext.createDataFrame(javaRDD, schema);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            dataset.show();
            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            try {
                dataset.write().format("parquet").mode("append").save("/sandbox/detail_data");
            } catch (Exception e) {
                System.out.println("----exception---" + e);
            }
        });


        ReportRDD.foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD -> {
            JavaRDD<Row> javaRDD = stringJavaRDD.map((Function<String, Row>) s -> {
                String[] data = s.split("\\|");
                return RowFactory.create(data[0], data[1], data[2], data[3], data[4],
                        data[5], data[6], data[7], data[8], data[9], data[10]);
            });
            // Gateway_Name,Message_ID,Message_Type,Calling_Number,Called_Number,Submission_Time,Final_End_Time,
            // Message_Status,Message_Status_Description,Message_Length,Details
            StructType schema = new StructType().add("GatewayName", "String")
                    .add("Message_ID", "String")
                    .add("Message_Type", "String")
                    .add("Calling_Number", "String")
                    .add("Called_Number", "String")
                    .add("Submission_Time", "String")
                    .add("Final_End_Time", "String")
                    .add("Message_Status", "String")
                    .add("Message_Status_Description", "String")
                    .add("Message_Length", "String")
                    .add("Details", "String");
            Dataset<Row> dataset = sqlContext.createDataFrame(javaRDD, schema);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            dataset.show();
            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$");

            dataset.write().format("parquet").mode("append").save("/sandbox/report_data/");
        });

        // 3. 写es
        ReportRDD.foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD ->
                stringJavaRDD.foreachPartition((VoidFunction<Iterator<String>>) stringIterator -> {
                    highLevelClient = getRestClient();
                    LocalDate localDate = LocalDate.now();

                    Map<String, Object> esMap = new HashMap<String, Object>();
                    String ReportDataIndex = "report_" + localDate.getYear() + "_" + localDate.getMonthValue();
                    int number = 0;
                    BulkRequest request = new BulkRequest();
                    // Gateway_Name,Message_ID,Message_Type,Calling_Number,Called_Number,Submission_Time,Final_End_Time,
                    // Message_Status,Message_Status_Description,Message_Length,Details
                    while (stringIterator.hasNext()) {
                        String[] s = stringIterator.next().split("\\|");
                        //攒批写es
                        number++;
                        esMap.put("Message_ID", s[1]);
                        esMap.put("Calling_Number", s[3]);
                        esMap.put("Called_Number", s[4]);
                        esMap.put("Submission_Time", s[5]);
                        esMap.put("Final_End_Time", s[6]);
                        request.add(new IndexRequest(ReportDataIndex).source(esMap));
                        if (number > 10) {
                            BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);
                            //CreateIndex.indexByMap(highLevelClient, DetailDataIndex,id,esMap);
                            if (RestStatus.OK.equals((bulkResponse.status()))) {
                                LOG.info("Bulk is successful");
                            } else {
                                LOG.info("Bulk is failed");
                            }
                            number = 0;
                        }
                    }
                    if (number != 0) {
                        BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);
                        // CreateIndex.indexByMap(highLevelClient, DetailDataIndex,id,esMap);
                        if (RestStatus.OK.equals((bulkResponse.status()))) {
                            LOG.info("Bulk is successful");
                        } else {
                            LOG.info("Bulk is failed");
                        }

                    }

                }));

        DetailRDD.foreachRDD((VoidFunction<JavaRDD<String>>) stringJavaRDD ->
                stringJavaRDD.foreachPartition((VoidFunction<Iterator<String>>) stringIterator -> {
                    highLevelClient = getRestClient();
                    LocalDate localDate = LocalDate.now();
                    Map<String, Object> esMap = new HashMap<String, Object>();
                    String DetailDataIndex = "detail_" + localDate.getYear() + "_" + localDate.getMonthValue();
                    int number = 0;
                    BulkRequest request = new BulkRequest();
                    // Date,Gateway_Name,Enterprise_Code,Business_Code,Service_Code,Send_Direction
                    while (stringIterator.hasNext()) {
                        String[] s = stringIterator.next().split("\\|");
                        // 攒批写es
                        number++;
                        esMap.put("Date", s[0]);
                        esMap.put("Gateway_Name", s[1]);
                        esMap.put("Enterprise_Code", s[2]);
                        esMap.put("Business_Code", s[3]);
                        request.add(new IndexRequest(DetailDataIndex).source(esMap));
                        if (number > 10) {
                            BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);
                            // CreateIndex.indexByMap(highLevelClient, DetailDataIndex,id,esMap);
                            if (RestStatus.OK.equals((bulkResponse.status()))) {
                                LOG.info("Bulk is successful");
                            } else {
                                LOG.info("Bulk is failed");
                            }
                            number = 0;
                        }
                    }
                    if (number != 0) {
                        BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);
                        // CreateIndex.indexByMap(highLevelClient, DetailDataIndex,id,esMap);
                        if (RestStatus.OK.equals((bulkResponse.status()))) {
                            LOG.info("Bulk is successful");
                        } else {
                            LOG.info("Bulk is failed");
                        }
                    }
                }));

        jssc.start();
        jssc.awaitTermination();
    }

    private static void printUsage() {
        System.out.println("Usage: {checkPointDir} {topic} {brokerList}");
        System.exit(1);
    }

    private static void login() throws IOException {
        if (User.isHBaseSecurityEnabled(conf)) {
            userName = "sandbox";
            String userdir = user_confdir;

            userKeytabFile = userdir + "user.keytab";
            krb5File = userdir + "krb5.conf";
            /*
             * if need to connect zk, please provide jaas info about zk. of course,
             * you can do it as below:
             * System.setProperty("java.security.auth.login.config", confDirPath +
             * "jaas.conf"); but the demo can help you more : Note: if this process
             * will connect more than one zk cluster, the demo may be not proper. you
             * can contact us for more help
             */
            LoginUtil.setJaasConf(ZOOKEEPER_DEFAULT_LOGIN_CONTEXT_NAME, userName, userKeytabFile);
            LoginUtil.login(userName, userKeytabFile, krb5File, conf);
        }
    }

    private static void init() throws IOException {
        // Default load from conf directory
        conf = HBaseConfiguration.create();
        // In Windows environment
        // String userdir = this.getClass().getClassLoader().getResource("conf").getPath() + File.separator;

        String userdir = user_confdir;

        // In linux environment
        // String userdir="/opt/sandbox/conf/";

        System.out.println("userdir: " + userdir);
        // In Linux environment
        // String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conf.addResource(new Path(userdir + "core-site.xml"), false);
        conf.addResource(new Path(userdir + "hdfs-site.xml"), false);
        conf.addResource(new Path(userdir + "hbase-site.xml"), false);
    }

    private static RestHighLevelClient getRestClient() {
        // 加载es-example.properties配置文件中的内容，包括与主机的连接信息，索引信息等
        LOG.info("Start to do high level rest client request !");
        try {
            // in Windows envrionment
            // String userdir = this.getClass().getClassLoader().getResource("conf").getPath() + File.separator;

            // in Linux environment
            // String userdir ="/opt/sandbox/conf/";

            // HwRestClient hwRestClient = new HwRestClient(userdir+"/conf/");
            HwRestClient hwRestClient = new HwRestClient();


            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            LOG.info("Start to do high level rest client request !" + highLevelClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return highLevelClient;
    }


    public static class ReportFunction implements Function<String, Put> {

        private static final long serialVersionUID = 1L;

        // Gateway_Name,Message_ID,Message_Type,Calling_Number,Called_Number,Submission_Time,
        // Final_End_Time,Message_Status,Message_Status_Description,Message_Length,Details
        @Override
        public Put call(String v) throws Exception {
            if (v.length() > 1) {
                String[] data = v.split("\\|");
                System.out.println("---------------------data.size:" + data.length);
                if (data.length < 11) {
                    return null;
                }

                Put put = new Put(Bytes.toBytes(String.format("%03d", (int) (Math.random() * 100)) + data[1]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Gateway_Name"),
                        Bytes.toBytes(data[0]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Message_ID"),
                        Bytes.toBytes(data[1]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Message_Type"),
                        Bytes.toBytes(data[2]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Calling_Number"),
                        Bytes.toBytes(data[3]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Called_Number"),
                        Bytes.toBytes(data[4]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Submission_Time"),
                        Bytes.toBytes(data[5]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Final_End_Time"),
                        Bytes.toBytes(data[6]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Message_Status"),
                        Bytes.toBytes(data[7]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Message_Status_Description"),
                        Bytes.toBytes(data[8]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Message_Length"),
                        Bytes.toBytes(data[9]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Details"),
                        Bytes.toBytes(data[10]));
                return put;
            } else {
                return null;
            }
        }
    }

    // Date,Gateway_Name,Enterprise_Code,Business_Code,Service_Code,Send_Direction
    private static class DetailFunction implements Function<String, Put> {
        @Override
        public Put call(String v) throws Exception {
            if (v.length() > 0) {
                System.out.println("------------v--------------" + v);
                String[] data = v.split("\\|");
                System.out.println("---------v--data.lengt--------------" + data.length);
                if (data.length < 6) {
                    return null;
                }
                System.out.println("---------------------------------data.size:" + data.length);
                Put put = new Put(Bytes.toBytes(
                        String.format("%03d", (int) (Math.random() * 100)) + data[0] + data[1] + data[2] + data[3]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Date"),
                        Bytes.toBytes(data[0]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Gateway_Name"),
                        Bytes.toBytes(data[1]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Enterprise_Code"),
                        Bytes.toBytes(data[2]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Business_Code"),
                        Bytes.toBytes(data[3]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Service_Code"),
                        Bytes.toBytes(data[4]));
                put.addColumn(Bytes.toBytes("base"),
                        Bytes.toBytes("Send_Direction"),
                        Bytes.toBytes(data[5]));
                return put;
            } else {
                return null;
            }
        }
    }
}
