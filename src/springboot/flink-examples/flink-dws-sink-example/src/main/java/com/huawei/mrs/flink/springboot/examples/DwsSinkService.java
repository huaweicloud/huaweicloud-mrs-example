/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.mrs.flink.springboot.examples;

import com.huawei.mrs.flink.springboot.examples.sink.DwsSink;
import com.huawei.mrs.flink.springboot.examples.source.GenIdSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class DwsSinkService {
    private static final Logger LOG = LoggerFactory.getLogger(DwsSinkService.class);

    @Autowired
    private DwsSink dwsSink;

    /**
     * 执行方法
     *
     * @throws Exception 异常
     */
    @PostConstruct
    public void execute() throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> source = streamEnv.addSource(new GenIdSource());
        source.print();
        source.addSink(dwsSink);

        new Thread(() -> {
            try {
                Thread.currentThread().setName("job-thread");
                streamEnv.execute("test");
            } catch (Exception e) {
                LOG.error("start job failed:e");
            }
        }).start();
        LOG.info("start job thread!");
    }
}
