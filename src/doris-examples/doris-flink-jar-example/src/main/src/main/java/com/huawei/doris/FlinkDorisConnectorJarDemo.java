package com.huawei.doris;

import java.io.InputStream;
import java.time.LocalDate;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.RowDataSerializer;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;

import java.util.Properties;

/**
 * 功能描述
 *
 * @since 2025-06-11
 */
public class FlinkDorisConnectorJarDemo {
    /**
     * 建表 语句
     * create database example_db;
     * CREATE TABLE `example_table` (
     * `city` varchar(256) NULL,
     * `longitude` double NULL,
     * `latitude` double NULL,
     * `destroy_date` date NULL
     * ) ENGINE=OLAP
     * DUPLICATE KEY(`city`)
     * DISTRIBUTED BY HASH(`city`) BUCKETS 3
     */
    private static String DATABASE = "example_db";
    private static String TABLE_NAME = "example_table";
    private static String FE_NODES = ""; // Leader Node host
    private static String USER = "";
    private static String PASSWD = "";

    public static void main(String[] args) throws Exception {
        Properties confProperties = new Properties();
        // 使用ClassLoader加载properties配置文件生成对应的输入流
        InputStream in = FlinkDorisConnectorJarDemo.class.getClassLoader().getResourceAsStream("conf.properties");
        // 使用properties对象加载输入流
        confProperties.load(in);
        //获取key对应的value值
        USER = confProperties.getProperty("USER");
        PASSWD = confProperties.getProperty("PASSWD");
        FE_NODES = confProperties.getProperty("FE_NODES");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        env.enableCheckpointing(10);
        env.setParallelism(1);

        // enable checkpoint
        env.enableCheckpointing(10000);
        // using batch mode for bounded data
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //doris sink option
        DorisSink.Builder<RowData> builder = DorisSink.builder();
        DorisOptions.Builder dorisBuilder = DorisOptions.builder();

        dorisBuilder.setFenodes(FE_NODES)
                .setTableIdentifier(DATABASE + "." + TABLE_NAME)
                .setUsername(USER)
                .setPassword(PASSWD)
                // 普通集群为false，安全集群为true
                .setIgnoreHttpsCA(false)
                // 普通集群为false，安全集群为true
                .setEnableHttps(false)
                // 默认值为true，如果flink jar在运行后报307，可将该值改为false
                .setAutoRedirect(false);

        // json format to streamload
        Properties properties = new Properties();

        properties.setProperty("format", "json");
        properties.setProperty("read_json_by_line", "false");
        DorisExecutionOptions.Builder executionBuilder = DorisExecutionOptions.builder();
        executionBuilder.setLabelPrefix("flink-label-doris") //streamload label prefix
                .setDeletable(false)
                .setStreamLoadProp(properties); //streamload params

        //flink rowdata‘s schema
        String[] fields = {"city", "longitude", "latitude", "destroy_date"};
        DataType[] types = {DataTypes.VARCHAR(256), DataTypes.DOUBLE(), DataTypes.DOUBLE(), DataTypes.DATE()};

        builder.setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionBuilder.build())
                .setSerializer(RowDataSerializer.builder()    //serialize according to rowdata
                        .setFieldNames(fields)
                        .setType("json")           //json format
                        .setFieldType(types).build())
                .setDorisOptions(dorisBuilder.build());

        //mock rowdata source
        DataStream<RowData> source = env.fromElements("")
                .map(new MapFunction<String, RowData>() {
                    @Override
                    public RowData map(String value) throws Exception {
                        GenericRowData genericRowData = new GenericRowData(4);
                        genericRowData.setField(0, StringData.fromString("ULC"));
                        genericRowData.setField(1, 21.59);
                        genericRowData.setField(2, 31.56);
                        genericRowData.setField(3, LocalDate.now().toEpochDay());
                        return genericRowData;
                    }
                });
        source.sinkTo(builder.build());
        env.execute("Flink DataStream example");
    }
}
