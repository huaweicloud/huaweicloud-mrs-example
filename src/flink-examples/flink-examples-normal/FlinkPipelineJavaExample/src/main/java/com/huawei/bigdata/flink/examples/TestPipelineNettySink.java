/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.netty.sink.NettySink;
import org.apache.flink.streaming.connectors.netty.utils.ZookeeperRegisterServerHandler;

/**
 * NettySink excmple
 * 
 * @since 2019/9/30
 */
public class TestPipelineNettySink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(2);

        ZookeeperRegisterServerHandler zkRegisterServerHandler = new ZookeeperRegisterServerHandler();

        env.addSource(new UserSource())
                .keyBy(0)
                .map(
                        new MapFunction<Tuple2<Integer, String>, byte[]>() {
                            @Override
                            public byte[] map(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                                return integerStringTuple2.f1.getBytes();
                            }
                        })
                .addSink(new NettySink("NettySink-1", "TOPIC-2", zkRegisterServerHandler, 2));

        env.execute();
    }
}
