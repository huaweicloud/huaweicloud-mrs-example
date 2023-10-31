/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.mrs.flink.springboot.examples.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

@Component
public class DwsSink extends RichSinkFunction<Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(DwsSink.class);

    private static final String UPDATE_SQL = "insert into test_lzh1(id) values (?)";

    @Value("${spring.datasource.dws.url}")
    private String dataSourceUrl;

    @Value("${spring.datasource.dws.username}")
    private String userName;

    @Value("${spring.datasource.dws.password}")
    private String password;

    @Value("${spring.datasource.dws.driver}")
    private String driverClass;

    private Connection conn = null;
    private PreparedStatement ps = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("init params:url={},userName={},driver={}", dataSourceUrl, userName, driverClass);
        //加载驱动,开启连接
        try {
            Class.forName(driverClass);
            conn = DriverManager.getConnection(dataSourceUrl, userName, password);
        } catch (Exception e) {
            LOG.error("get connection failed:", e);
            throw e;
        }
    }

    @Override
    public void invoke(Integer source, Context context) {
        try {
            ps = conn.prepareStatement(UPDATE_SQL);
            ps.setInt(1, source);
            ps.executeUpdate();
        } catch (Exception e) {
            LOG.error("invoke failed:", e);
        }
    }

    /**
     * 结束任务,关闭连接
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
        if (ps != null) {
            ps.close();
        }
    }
}
