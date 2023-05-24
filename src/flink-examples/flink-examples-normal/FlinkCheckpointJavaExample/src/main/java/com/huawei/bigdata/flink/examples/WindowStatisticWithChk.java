/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * 窗口统计Function
 * 
 * @since 2019/9/30
 */
public class WindowStatisticWithChk
        implements WindowFunction<Tuple4<Long, String, String, Integer>, Long, Tuple, TimeWindow>,
                ListCheckpointed<UDFState> {
    private Long total = 0L;

    @Override
    public List<UDFState> snapshotState(long l, long l1) throws Exception {
        List<UDFState> listState = new ArrayList<UDFState>();
        UDFState udfState = new UDFState();
        udfState.setState(total);
        listState.add(udfState);
        return listState;
    }

    @Override
    public void restoreState(List<UDFState> list) throws Exception {
        UDFState udfState = list.get(0);
        total = udfState.getState();
    }

    @Override
    public void apply(
            Tuple tuple,
            TimeWindow timeWindow,
            Iterable<Tuple4<Long, String, String, Integer>> iterable,
            Collector<Long> collector)
            throws Exception {
        long count = 0L;
        for (Tuple4<Long, String, String, Integer> event : iterable) {
            count++;
        }
        total += count;
        collector.collect(count);
    }
}
