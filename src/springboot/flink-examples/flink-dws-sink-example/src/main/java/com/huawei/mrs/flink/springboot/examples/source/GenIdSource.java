/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.mrs.flink.springboot.examples.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class GenIdSource extends RichParallelSourceFunction<Integer> {
    // flag indicating whether source is still running
    private boolean running = true;

    @Override
    public void run(SourceContext<Integer> sourceContext) throws Exception {
        // initialize random number generator
        Random rand = new Random();

        while (running) {
            int res = rand.nextInt(10000);
            sourceContext.collect(res);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
