/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.mrs.flink.springboot.examples;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.annotation.PostConstruct;

@Component
public class DwsSinkService {
    private static final Logger LOG = LoggerFactory.getLogger(DwsSinkService.class);

    @Value("${spring.datasource.dws.url}")
    private String dataSourceUrl;

    @Value("${spring.datasource.dws.password}")
    private String password;

    @Value("${spring.datasource.dws.table-name-source}")
    private String tablenameSource;

    @Value("${spring.datasource.dws.table-name-sink}")
    private String tablenameSink;

    @Value("${spring.datasource.dws.username}")
    private String username;

    @Value("${spring.datasource.dws.autoFlushBatchSize}")
    private String sinkAutoFlushBatchSize;

    /**
     * 执行方法
     *
     * @throws Exception 异常
     */
    @PostConstruct
    public void execute() throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 开启Flink CheckPoint配置，开启时若触发CheckPoint，会将Offset信息同步到Kafka
        env.enableCheckpointing(30 * 1000);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String sourceDws =
                "create table dataGenSource(\n" +
                        "  id int\n" +
                        ") with (\n" +
                        "   'connector' = 'dws',\n" +
                        "   'url' = '" + dataSourceUrl + "',\n" +
                        "   'table-name' = '" + tablenameSource + "',\n" +
                        "   'username' = '" + username + "',\n" +
                        "   'password' = '" + password + "'\n" +
                        ")";
        tEnv.executeSql(sourceDws);


        String sinkDws =
                "CREATE TABLE dws_output(\n" +
                        "  id int\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   'connector' = 'dws',\n" +
                        "   'url' = '" + dataSourceUrl + "',\n" +
                        "   'table-name' = '" + tablenameSink +"',\n" +
                        "   'username' = '" + username + "',\n" +
                        "   'password' = '" + password + "',\n" +
                        "   'autoFlushBatchSize' = '" + sinkAutoFlushBatchSize + "'\n" +
                        ")";
        tEnv.executeSql(sinkDws);

        String query =
                "insert into dws_output \n" +
                        "select id from dataGenSource\n";
        tEnv.executeSql(query);

        new Thread(() -> {
            try {
                Thread.currentThread().setName("job-thread");
                streamEnv.execute("test");
            } catch (Exception e) {
                LOG.error("start job failed:e");
            }
        }).start();
        LOG.info("start job thread!");
    }
}
