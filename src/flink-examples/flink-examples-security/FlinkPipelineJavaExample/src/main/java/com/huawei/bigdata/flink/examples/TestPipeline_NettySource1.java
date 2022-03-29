/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.netty.source.NettySource;
import org.apache.flink.streaming.connectors.netty.utils.ZookeeperRegisterServerHandler;

/**
 * @since 8.0.2
 */
public class TestPipeline_NettySource1 {
    /**
     * @param args args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        ZookeeperRegisterServerHandler zkRegisterServerHandler = new ZookeeperRegisterServerHandler();
        env.addSource(new NettySource("NettySource-1", "TOPIC-2", zkRegisterServerHandler))
                .map(
                        new MapFunction<byte[], String>() {
                            @Override
                            public String map(byte[] bt) {
                                return new String(bt);
                            }
                        })
                .print();

        env.execute();
    }
}
