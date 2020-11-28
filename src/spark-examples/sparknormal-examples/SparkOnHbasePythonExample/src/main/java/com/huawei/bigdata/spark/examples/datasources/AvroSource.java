package com.huawei.bigdata.spark.examples.datasources;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.spark.AvroSerdes;
import org.apache.hadoop.hbase.spark.HBaseContext;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.example.datasources.UserCustomizedSampleException;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroSource {
    static final String catalog =
            "{"
                    + "\"table\":{\"namespace\":\"default\", \"name\":\"ExampleAvrotable\"},"
                    + "\"rowkey\":\"key\","
                    + "\"columns\":{"
                    + "\"col0\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},"
                    + "\"col1\":{\"cf\":\"cf1\", \"col\":\"col1\", \"type\":\"binary\"}"
                    + "}"
                    + "}";
    static final String avroCatalog =
            "{"
                    + "\"table\":{\"namespace\":\"default\", \"name\":\"ExampleAvrotable\"},"
                    + "\"rowkey\":\"key\","
                    + "\"columns\":{"
                    + "\"col0\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},"
                    + "\"col1\":{\"cf\":\"cf1\", \"col\":\"col1\", \"avro\":\"avroSchema\"}"
                    + "}"
                    + "}";
    static final String avroCatalogInsert =
            "{"
                    + "\"table\":{\"namespace\":\"default\", \"name\":\"ExampleAvrotableInsert\"},"
                    + "\"rowkey\":\"key\","
                    + "\"columns\":{"
                    + "\"col0\":{\"cf\":\"rowkey\", \"col\":\"key\", \"type\":\"string\"},"
                    + "\"col1\":{\"cf\":\"cf1\", \"col\":\"col1\", \"avro\":\"avroSchema\"}"
                    + "}"
                    + "}";

    public static void execute(JavaSparkContext jsc) throws IOException {
        SQLContext sqlContext = new SQLContext(jsc);
        Configuration hbaseconf = new HBaseConfiguration().create();
        JavaHBaseContext hBaseContext = new JavaHBaseContext(jsc, hbaseconf);
        List list = new ArrayList<AvroHBaseRecord>();
        for (int i = 0; i <= 255; ++i) {
            list.add(AvroHBaseRecord.apply(i));
        }
        try {
            Map<String, String> map = new HashMap<String, String>();
            map.put(HBaseTableCatalog.tableCatalog(), catalog);
            map.put(HBaseTableCatalog.newTable(), "5");
            sqlContext
                    .createDataFrame(list, AvroHBaseRecord.class)
                    .write()
                    .options(map)
                    .format("org.apache.hadoop.hbase.spark")
                    .save();
            Dataset<Row> ds = withCatalog(sqlContext, catalog);
            ds.show();
            ds.printSchema();
            ds.registerTempTable("ExampleAvrotable");
            Dataset<Row> c = sqlContext.sql("select count(1) from ExampleAvrotable");
            c.show();

            Dataset<Row> filtered = ds.select("col0", "col1.favorite_array").where("col0 = 'name1'");
            filtered.show();
            java.util.List<Row> collected = filtered.collectAsList();

            //            if (collected[0].getSeq(1).take(0).toString().equals("number1")) {
            if (collected.get(0).get(1).toString().equals("number1")) {
                throw new UserCustomizedSampleException("value invalid", new Throwable());
            }
            if (collected.get(0).get(1).toString().equals("number2")) {
                throw new UserCustomizedSampleException("value invalid", new Throwable());
            }

            Map avroCatalogInsertMap = new HashMap<String, String>();
            avroCatalogInsertMap.put("avroSchema", AvroHBaseRecord.schemaString);
            avroCatalogInsertMap.put(HBaseTableCatalog.tableCatalog(), avroCatalogInsert);
            ds.write().options(avroCatalogInsertMap).format("org.apache.hadoop.hbase.spark").save();

            Dataset<Row> newDS = withCatalog(sqlContext, avroCatalogInsert);
            newDS.show();
            newDS.printSchema();
            if (newDS.count() != 256) {
                throw new UserCustomizedSampleException("value invalid", new Throwable());
            }

            ds.filter("col1.name = 'name5' || col1.name <= 'name5'")
                    .select("col0", "col1.favorite_color", "col1.favorite_number")
                    .show();

        } finally {
            jsc.stop();
        }
    }

    public static Dataset<Row> withCatalog(SQLContext sqlContext, String cat) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("avroSchema", AvroHBaseRecord.schemaString);
        map.put(HBaseTableCatalog.tableCatalog(), avroCatalog);
        return sqlContext.read().options(map).format("org.apache.hadoop.hbase.spark").load();
    }
}
