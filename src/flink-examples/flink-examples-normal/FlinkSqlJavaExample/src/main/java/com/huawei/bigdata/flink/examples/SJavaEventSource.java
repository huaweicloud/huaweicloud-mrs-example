/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * 自定义source类
 * 
 * @since 2019/9/30
 */
public class SJavaEventSource extends RichSourceFunction<Tuple4<Long, String, String, Integer>> {
    private Long count = 0L;
    private boolean isRunning = true;
    private String alphabet = "abcdefg";

    @Override
    public void run(SourceContext<Tuple4<Long, String, String, Integer>> ctx) throws Exception {
        while (isRunning) {
            for (long i = 0; i < 10; i++) {
                ctx.collect(Tuple4.of(i, "hello-" + count, alphabet, 1));
                count++;
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
