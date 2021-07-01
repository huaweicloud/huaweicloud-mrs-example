/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

/**
 * 自定义带状态Source类
 *
 * @since 2019/9/30
 */
public class SEventSourceWithChk extends RichSourceFunction<Tuple4<Long, String, String, Integer>>
    implements CheckpointedFunction {
    private Long count = 0L;
    private boolean isRunning = true;
    List<UDFState> listState = new ArrayList<UDFState>();
    private String alphabet =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWZYX0987654321";

    @Override
    public void run(SourceContext<Tuple4<Long, String, String, Integer>> ctx) throws Exception {
        SecureRandom random = new SecureRandom();
        while (isRunning) {
            for (int i = 0; i < 10000; i++) {
                ctx.collect(Tuple4.of(random.nextLong(), "hello-" + count, alphabet, 1));
                count++;
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        UDFState udfState = new UDFState();
        udfState.setState(count);
        listState.add(udfState);
    }

    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        if (listState.size() > 0) {
            UDFState udfState = listState.get(0);
            count = udfState.getState();
        }
    }
}
