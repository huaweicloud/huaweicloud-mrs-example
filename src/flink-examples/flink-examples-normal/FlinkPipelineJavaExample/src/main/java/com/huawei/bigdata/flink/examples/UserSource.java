/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;

/**
 * user custom source
 * 
 * @since 2019/9/30
 */
public class UserSource extends RichParallelSourceFunction<Tuple2<Integer, String>> implements Serializable {
    private boolean isRunning = true;

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
        while (isRunning) {
            for (int i = 0; i < 10000; i++) {
                ctx.collect(Tuple2.of(i, "hello-" + i));
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void close() {
        isRunning = false;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
