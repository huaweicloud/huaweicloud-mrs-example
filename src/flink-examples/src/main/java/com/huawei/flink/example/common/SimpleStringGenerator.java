package com.huawei.flink.example.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SimpleStringGenerator implements SourceFunction<String> {

    private static final long serialVersionUID = 2174904787118597072L;

    boolean running = true;
    long i = 0;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception
    {
        while (running) {
            sourceContext.collect("element-" + (i++));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel()
    {
        running = false;
    }
}
