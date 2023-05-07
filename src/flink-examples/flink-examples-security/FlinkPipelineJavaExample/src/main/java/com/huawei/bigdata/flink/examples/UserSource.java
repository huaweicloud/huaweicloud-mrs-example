/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.Serializable;

/**
 * @since 8.0.2
 */
public class UserSource extends RichParallelSourceFunction<Tuple2<Integer, String>> implements Serializable {
    private boolean isRunning = true;

    /**
     * @param configuration configuration
     * @throws Exception
     */
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
    }

    /**
     * @param ctx SourceContext
     * @throws Exception
     */
    public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
        while (isRunning) {
            for (int i = 0; i < 10000; i++) {
                ctx.collect(Tuple2.of(i, "hello-" + i));
            }
            Thread.sleep(1000);
        }
    }

    /**
     * @since 8.0.2
     */
    public void close() {
        isRunning = false;
    }

    /**
     * @since 8.0.2
     */
    public void cancel() {
        isRunning = false;
    }
}
