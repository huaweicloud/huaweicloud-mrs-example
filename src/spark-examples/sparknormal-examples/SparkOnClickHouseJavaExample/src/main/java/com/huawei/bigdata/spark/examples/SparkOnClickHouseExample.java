package com.huawei.bigdata.spark.examples;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseDriver;
import com.clickhouse.jdbc.ClickHouseStatement;

/**
 * 功能描述 SparkOnClickHouseJavaExample
 *
 * @since 2022-03-17
 */
public class SparkOnClickHouseExample {

    private static final String DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

    public static void main(String[] args) throws SQLException {
        if (args.length < 3) {
            System.out.println("Missing parameters, need ckJdbcUrl, ckDBName, ckTableName");
            System.exit(0);
        }

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkOnClickHouseJavaExample")
                .enableHiveSupport()
                .getOrCreate();

        String jdbcUrl = args[0];
        String ckDBName = args[1];
        String ckTableName = args[2];
        String user = args[3];

        Properties props = new Properties();
        props.put("driver", DRIVER);
        props.put("user", user);

        ClickHouseDriver ckDriver = new ClickHouseDriver();
        ClickHouseConnection ckConnect = ckDriver.connect(jdbcUrl, props);
        ClickHouseStatement ckStatement = ckConnect.createStatement();

        String allDBs = "show databases";
        ResultSet rs = ckStatement.executeQuery(allDBs);
        while (rs.next()) {
            String dbName = rs.getString(1);
            System.out.println("dbName: " + dbName);
        }

        Long startTime = System.currentTimeMillis();
        clickHouseExecute(ckStatement, ckDBName, ckTableName);
        System.out.println("[Elapsed]" + (System.currentTimeMillis() - startTime));

        Dataset ckData = spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("user", user)
                .option("driver", DRIVER)
                .option("dbtable", ckDBName+"."+ckTableName)
                .load();

        ckData.show();

        ckData.registerTempTable("ckTempTable");

        Dataset newCkData = spark.sql("select EventDate,cast(id+20 as decimal(20,0)) as id,name,age,address from ckTempTable");

        newCkData.show();
        newCkData.write().mode("Append").jdbc(jdbcUrl, ckDBName + "." + ckTableName, props);

        spark.stop();
    }

    private static void clickHouseExecute(ClickHouseStatement ckStatement, String clickHouseDB, String clickHouseTable)
            throws SQLException {
        String createDB = "CREATE DATABASE " + clickHouseDB + " ON CLUSTER default_cluster";
        String createTable = "CREATE TABLE " + clickHouseDB + "." + clickHouseTable + " ON CLUSTER default_cluster " +
                "(`EventDate` DateTime,`id` UInt64,`name` String,`age` UInt8,`address` String)" +
                "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/" + clickHouseDB + "/" + clickHouseTable + "', '{replica}')" +
                "PARTITION BY toYYYYMM(EventDate) ORDER BY id";

        String insertData = "insert into " + clickHouseDB + "." + clickHouseTable + " (id, name,age,address) values (1, 'zhangsan',17,'xian'), (2, 'lisi',36,'beijing')";

        ckStatement.execute(createDB);
        ckStatement.execute(createTable);
        ckStatement.execute(insertData);
    }
}
