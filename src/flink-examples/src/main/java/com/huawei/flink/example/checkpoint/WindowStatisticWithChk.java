package com.huawei.flink.example.checkpoint;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class WindowStatisticWithChk implements WindowFunction<Tuple4<Long, String, String, Integer>, Long, Tuple, TimeWindow>, ListCheckpointed<UDFState> {

    private long total = 0;

    @Override
    public List<UDFState> snapshotState(long l, long l1) throws Exception
    {
        UDFState udfState = new UDFState();
        List<UDFState> list = new ArrayList<UDFState>();
        udfState.setCount(total);
        list.add(udfState);
        return list;
    }

    @Override
    public void restoreState(List<UDFState> list) throws Exception
    {
        UDFState udfState = list.get(0);
        total = udfState.getCount();
    }

    @Override
    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple4<Long, String, String, Integer>> iterable, Collector<Long> collector) throws Exception
    {
        long count = 0L;
        for (Tuple4<Long, String, String, Integer> tuple4 : iterable) {
            count ++;
        }
        total += count;
        collector.collect(total);
    }
}

