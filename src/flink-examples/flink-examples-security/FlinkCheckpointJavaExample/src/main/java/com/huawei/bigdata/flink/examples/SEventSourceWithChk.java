/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

/**
 * @since 8.0.2
 */
public class SEventSourceWithChk extends RichSourceFunction<Tuple4<Long, String, String, Integer>>
        implements ListCheckpointed<UDFState> {
    private Long count = 0L;
    private boolean isRunning = true;
    private String alphabet =
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" +
                    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWZYX0987654321";

    /**
     * @param ctx param1
     * @throws Exception
     */
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

    /**
     * @since 8.0.2
     */
    public void cancel() {
        isRunning = false;
    }

    /**
     * @param l param1
     * @param ll param2
     * @return snapshot list
     * @throws Exception
     */
    public List<UDFState> snapshotState(long l, long ll) throws Exception {
        UDFState udfState = new UDFState();
        List<UDFState> listState = new ArrayList<UDFState>();
        udfState.setState(count);
        listState.add(udfState);

        return listState;
    }

    /**
     * @param list restore list
     * @throws Exception
     */
    public void restoreState(List<UDFState> list) throws Exception {
        UDFState udfState = list.get(0);
        count = udfState.getState();
    }
}
