/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.mrs.flink.springboot.examples.source;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class GenStrSource extends RichParallelSourceFunction<String> {
    // flag indicating whether source is still running
    private boolean running = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        // initialize random number generator
        Random rand = new Random();

        while (running) {
            String res = getRandomString(20);
            sourceContext.collect(res);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }

    private String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random=new Random();
        StringBuffer sb = new StringBuffer();
        for(int i=0; i<length; i++){
            int number = random.nextInt(62);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }
}
