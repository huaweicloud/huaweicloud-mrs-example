package com.huawei.bigdata.spark.examples.datasources;

import com.esotericsoftware.kryo.Kryo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseSource {
    static final String cat =
            "{\n"
                    + "\"table\":{\"namespace\":\"default\", \"name\":\"HBaseSourceExampleTable\"},\n"
                    + "\"rowkey\":\"key\",\n"
                    + "\"columns\":{\n"
                    + "\"key\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},\n"
                    + "\"col1\":{\"cf\":\"cf1\", \"col\":\"col1\", \"type\":\"boolean\"},\n"
                    + "\"col2\":{\"cf\":\"cf2\", \"col\":\"col2\", \"type\":\"double\"},\n"
                    + "\"col3\":{\"cf\":\"cf3\", \"col\":\"col3\", \"type\":\"float\"},\n"
                    + "\"col4\":{\"cf\":\"cf4\", \"col\":\"col4\", \"type\":\"int\"},\n"
                    + "\"col5\":{\"cf\":\"cf5\", \"col\":\"col5\", \"type\":\"bigint\"},\n"
                    + "\"col6\":{\"cf\":\"cf6\", \"col\":\"col6\", \"type\":\"smallint\"},\n"
                    + "\"col7\":{\"cf\":\"cf7\", \"col\":\"col7\", \"type\":\"string\"}\n"
                    + "}\n"
                    + "}";

    public static Dataset withCatalog(SQLContext sqlContext, String cat) {
        Map map = new HashMap<String, String>();
        map.put(HBaseTableCatalog.tableCatalog(), cat);
        return sqlContext.read().options(map).format("org.apache.hadoop.hbase.spark").load();
    }

    public static void main(String args[]) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("HBaseSourceExample");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        Configuration conf = HBaseConfiguration.create();
        JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);
        try {
            List<HBaseRecord> list = new ArrayList<HBaseRecord>();
            for (int i = 0; i < 256; i++) {
                list.add(new HBaseRecord(i));
            }
            Map map = new HashMap<String, String>();
            map.put(HBaseTableCatalog.tableCatalog(), cat);
            map.put(HBaseTableCatalog.newTable(), "5");
            System.out.println("Before insert data into hbase table");
            sqlContext
                    .createDataFrame(list, HBaseRecord.class)
                    .write()
                    .options(map)
                    .format("org.apache.hadoop.hbase.spark")
                    .save();
            Dataset<Row> ds = withCatalog(sqlContext, cat);
            System.out.println("After insert data into hbase table");
            ds.printSchema();
            ds.show();
            ds.filter("key <= 'row5'").select("key", "col1").show();
            ds.registerTempTable("table1");
            Dataset<Row> tempDS = sqlContext.sql("select count(col1) from table1 where key < 'row5'");
            tempDS.show();
        } finally {
            jsc.stop();
        }
    }
}
