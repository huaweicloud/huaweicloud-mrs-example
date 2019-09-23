package com.huawei.flink.example.checkpoint;


import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SimpleSourceWithCheckPoint implements SourceFunction<Tuple4<Long, String, String, Integer>>, ListCheckpointed<UDFState> {

    private long count = 0;
    private boolean isRunning = true;
    private String alphabet = "justtest";

    @Override
    public List<UDFState> snapshotState(long l, long l1) throws Exception
    {
        UDFState udfState = new UDFState();
        List<UDFState> udfStateList = new ArrayList<UDFState>();
        udfState.setCount(count);
        udfStateList.add(udfState);
        return udfStateList;
    }

    @Override
    public void restoreState(List<UDFState> list) throws Exception
    {
        UDFState udfState = list.get(0);
        count = udfState.getCount();
    }

    @Override
    public void run(SourceContext<Tuple4<Long, String, String, Integer>> sourceContext) throws Exception
    {
        Random random = new Random();
        while (isRunning) {
            for (int i = 0; i < 10000; i++) {
                sourceContext.collect(Tuple4.of(random.nextLong(), "hello" + count, alphabet, 1));
                count ++;
            }
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel()
    {
        isRunning = false;
    }
}

