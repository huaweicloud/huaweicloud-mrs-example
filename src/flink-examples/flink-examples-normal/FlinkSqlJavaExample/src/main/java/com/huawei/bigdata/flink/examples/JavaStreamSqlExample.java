/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 流转sql示例类
 * 
 * @since 2019/9/30
 */
public class JavaStreamSqlExample {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "bin/flink run --class com.huawei.bigdata.flink.examples.JavaStreamSqlExample "
                        + "<path of StreamSqlExample jar> ");
        System.out.println("*********************************************************************************");

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, fsSettings);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(2);

        DataStream<Tuple4<Long, String, String, Integer>> ds1 = env.addSource(new SJavaEventSource());
        DataStream<Tuple4<Long, String, String, Integer>> ds2 = env.addSource(new SJavaEventSource());

        Table in1 = tEnv.fromDataStream(ds1, $("id"), $("name"), $("info"), $("cnt"));
        tEnv.createTemporaryView("SEvent1", in1);

        Table in2 = tEnv.fromDataStream(ds2, $("id"), $("name"), $("info"), $("cnt"));
        tEnv.createTemporaryView("SEvent2", in2);

        Table tmpStream =
                tEnv.sqlQuery(
                        "SELECT * FROM SEvent1 where id > 7 " + " UNION ALL " + " SELECT * FROM SEvent2 where id > 7");

        TupleTypeInfo tupType =
                new TupleTypeInfo(
                        BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

        DataStream<Tuple4<Long, String, String, Integer>> tmpDs = tEnv.toAppendStream(tmpStream, tupType);
        Table tmpTable = tEnv.fromDataStream(tmpDs, $("id"), $("name"), $("info"), $("cnt"), $("proctime").proctime());
        tEnv.createTemporaryView("TmpStream", tmpTable);

        Table result = tEnv.sqlQuery("SELECT sum(cnt) from TmpStream GROUP BY TUMBLE(proctime, interval '10' second)");
        tEnv.toAppendStream(result, Integer.class).print();

        env.execute();
    }
}
