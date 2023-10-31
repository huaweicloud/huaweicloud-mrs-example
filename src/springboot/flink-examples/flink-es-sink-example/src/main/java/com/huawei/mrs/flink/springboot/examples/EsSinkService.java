/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.mrs.flink.springboot.examples;

import com.huawei.mrs.flink.springboot.examples.source.GenStrSource;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component
public class EsSinkService {
    private static final Logger LOG = LoggerFactory.getLogger(EsSinkService.class);

    @Value("${spring.datasource.es.host}")
    private String host;

    @Value("${spring.datasource.es.port}")
    private Integer port;

    @Value("${spring.datasource.es.scheme}")
    private String scheme;

    /**
     * 执行方法
     *
     * @throws Exception 异常
     */
    @PostConstruct
    public void execute() throws Exception {
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.enableCheckpointing(60000);

        DataStream<String> source = streamEnv.addSource(new GenStrSource());

        source.sinkTo(
                new Elasticsearch7SinkBuilder<String>()
                        .setBulkFlushMaxActions(1) // Instructs the sink to emit after every element, otherwise they would be buffered
                        .setHosts(new HttpHost(host, port, scheme))
                        .setEmitter(
                                (element, context, indexer) ->
                                        indexer.add(createIndexRequest(element)))
                        .build()
        );

        new Thread(() -> {
            try {
                Thread.currentThread().setName("job-thread");
                streamEnv.execute("esTest");
            } catch (Exception e) {
                LOG.error("start job failed:e");
            }
        }).start();
        LOG.info("start job thread!");
    }

    private static IndexRequest createIndexRequest(String element) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("index2")
                .id(element)
                .source(json);
    }
}
