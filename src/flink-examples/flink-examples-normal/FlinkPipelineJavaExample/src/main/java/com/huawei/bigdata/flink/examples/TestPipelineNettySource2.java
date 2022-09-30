/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.netty.source.NettySource;
import org.apache.flink.streaming.connectors.netty.utils.ZookeeperRegisterServerHandler;

import java.nio.charset.Charset;

/**
 * NettySource excmple 2
 * 
 * @since 2019/9/30
 */
public class TestPipelineNettySource2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        ZookeeperRegisterServerHandler zkRegisterServerHandler = new ZookeeperRegisterServerHandler();
        env.addSource(new NettySource("NettySource-2", "TOPIC-2", zkRegisterServerHandler))
                .map(
                        new MapFunction<byte[], String>() {
                            @Override
                            public String map(byte[] bytes) {
                                return new String(bytes, Charset.forName("UTF-8"));
                            }
                        })
                .print();

        env.execute();
    }
}
