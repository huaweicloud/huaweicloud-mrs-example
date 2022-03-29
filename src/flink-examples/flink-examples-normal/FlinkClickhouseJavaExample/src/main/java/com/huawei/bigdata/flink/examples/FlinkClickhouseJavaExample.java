/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */
package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

public class FlinkClickhouseJavaExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //连接Clickhouse的参数
        String driver = "ru.yandex.clickhouse.ClickHouseDriver";
        /* 安全模式使用https端口 url = "jdbc:clickhouse://192.168.0.183:21426/default?&ssl=true&sslmode=none" */
        String url = "jdbc:clickhouse://192.168.0.183:8123/default";
        String user = "default";
        String password = "";
        String sqlRead = "select * from students";
        String sqlWrite = "insert into students(name, city, id, age) values(?, ?, ?, ?)";

        DataSet<Row> value = env.fromElements(new Tuple4<>("Liming", "BeiJing", 0, 24))
                .map(t -> {
                    Row row = new Row(4);
                    row.setField(0, t.f0);
                    row.setField(1, t.f1);
                    row.setField(2, t.f2);
                    row.setField(3, t.f3);
                    return row;
                });

        //调用写入Clickhouse的方法
        writeClickhouse(env, value, url, driver, user, password, sqlWrite);

        //从Clickhouse中读取数据
        readClickhouse(env, url, driver, user, password, sqlRead);
    }

    static void readClickhouse(ExecutionEnvironment env, String url, String driver,
                               String user, String pwd, String sql) throws Exception {
        DataSet<Row> dataResult = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(user)
                .setPassword(pwd)
                .setQuery(sql)
                .setRowTypeInfo(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO))
                .finish());
        dataResult.print();
    }

    static void writeClickhouse(ExecutionEnvironment env, DataSet<Row> outputData, String url,
                                String driver, String user, String pwd, String sql) throws Exception {
        outputData.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(user)
                .setPassword(pwd)
                .setQuery(sql)
                .finish());

        env.execute("insert into clickhouse job");
        System.out.println("data write successfully");
    }
}
