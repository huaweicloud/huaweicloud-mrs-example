package com.huawei.mrs.demo.board;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.joda.time.DateTime;
import com.huawei.mrs.demo.board.util.KafkaParams;
import com.huawei.mrs.demo.board.util.MysqlParams;
import com.huawei.mrs.demo.board.writer.JavaMysqlWriter;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class JavaEntry {
    private static final String STREAM_TABLE_NAME = "source";
    private static final String QUERY_PAY_HISTORY = "pay_history";
    private static final String QUERY_PAY_TODAY = "pay_today";

    private SparkSession spark = null;

    static String loadSQLFromResource(String fileName) throws IOException {
        final URL resource = JavaEntry.class.getResource(fileName);
        return Resources.toString(resource, Charsets.UTF_8);
    }

    void run() throws IOException {
        // list to store streaming queries
        List<StreamingQuery> queryList = new ArrayList<>();
        String dateString = DateTime.now().toString("yyyy-MM-dd");

        // 1. create spark session
        spark = SparkSession
                .builder()
                .config("spark.sql.shuffle.partitions", 2)
                .getOrCreate();

        // 2. read from kafka，and register stream table
        final Dataset<Row> dataset = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", KafkaParams.BROKERS)
                .option("subscribe", KafkaParams.TOPIC)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST (key AS STRING) key", "CAST(value AS STRING) value")
                .selectExpr("json_tuple(value, 'table', 'type', 'data') AS (table, type, data)");
        dataset.createOrReplaceTempView(STREAM_TABLE_NAME);

        // 3.1 create stream query to calculate total pay today
        String payTodaySQL = loadSQLFromResource("/pay_today_streaming.sql");
        String insertSQL = loadSQLFromResource("/pay_today_insert.sql");
        queryList.add(startQuery(QUERY_PAY_TODAY, payTodaySQL, new JavaMysqlWriter(MysqlParams.JDBC_URL, MysqlParams.USER, MysqlParams.PASSWORD, insertSQL, MysqlParams.COL_PAY_TODAY)));

        // 3.2 calculate total all-time pay
        // 3.2.1 get total history pay before today
        final Dataset<Row> orderBatchTable = getBaseMysqlDataFrameReader()
                .option("dbtable", String.format("`%s`.`%s`", MysqlParams.DATABASE_NAME, MysqlParams.TABLE_ORDERS))
        // set partitionColumn, lowerBound, upperBound, numPartitions to read parallel
                .load();
        orderBatchTable.createOrReplaceTempView(MysqlParams.TABLE_ORDERS);

        String payHistoryBatchSql = loadSQLFromResource("/pay_history_batch.sql");
        double payHistory = spark.sql(payHistoryBatchSql).first().getDouble(0);

        String payHistorySQL = String.format(loadSQLFromResource("/pay_history_streaming.sql"), payHistory, dateString);

        String payHistoryInsertSQL = loadSQLFromResource("/pay_history_insert.sql");
        queryList.add(startQuery(QUERY_PAY_HISTORY, payHistorySQL, new JavaMysqlWriter(MysqlParams.JDBC_URL, MysqlParams.USER, MysqlParams.PASSWORD, payHistoryInsertSQL, MysqlParams.COL_PAY_HISTORY)));

        queryList.forEach(query -> {
            try {
                query.awaitTermination();
            } catch (StreamingQueryException e) {
                e.printStackTrace();
            }
        });

        spark.stop();

    }

    private StreamingQuery startQuery(String queryName, String sql, ForeachWriter writer) {
        return spark.sql(sql)
                .writeStream()
                .queryName(queryName)
                .outputMode("update")
                .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
                .foreach(writer)
                .start();
    }

    private DataFrameReader getBaseMysqlDataFrameReader() {
        return spark.read().format("jdbc")
                .option("url", MysqlParams.JDBC_URL)
                .option("user", MysqlParams.USER)
                .option("password", MysqlParams.PASSWORD);
    }

    public static void main(String[] args) throws Exception {
        new JavaEntry().run();
    }
}
