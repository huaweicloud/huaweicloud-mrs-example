/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.multithread;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 多线程请求
 *
 * @since 2020-09-30
 */
public class MultithreadRequest {
    private static final Logger LOG = LogManager.getLogger(MultithreadRequest.class);

    /**
     * Bulk request can be used to to execute multiple index,update or delete
     * operations using a single request.
     */
    private static void bulk(RestHighLevelClient highLevelClient, String index) {
        try {
            BulkRequest request = new BulkRequest();
            BulkResponse bulkResponse;
            Map<String, Object> jsonMap = new HashMap<>();
            // 需要写入的总文档数
            long totalRecordNum = 10000;
            // 一次bulk写入的文档数,推荐一次写入的大小为5M-15M
            long oneCommit = 1000;
            long circleNumber = totalRecordNum / oneCommit;
            for (int i = 1; i <= circleNumber; i++) {
                jsonMap.clear();
                for (int j = 0; j < oneCommit; j++) {
                    jsonMap.put("user", "Linda");
                    jsonMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                    jsonMap.put("postDate", "2020-01-01");
                    jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                    jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                    jsonMap.put("uid", i);
                }
                request.add(new IndexRequest(index).source(jsonMap));
                bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);
                if (RestStatus.OK.equals((bulkResponse.status()))) {
                    // bulk请求中如果有失败的条目，请在业务代码中进行重试处理
                    if (bulkResponse.hasFailures()) {
                        LOG.warn("There are some failures in bulk response,please retry in client.");
                    } else {
                        LOG.info("Bulk successful");
                    }
                } else {
                    LOG.warn("Bulk failed.");
                }
            }
        } catch (IOException e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

    /**
     * The thread that send a bulk request
     */
    public static class SendRequestThread implements Runnable {
        private RestHighLevelClient highLevelClientTh;

        private String[] args;

        public SendRequestThread(String[] args) {
            this.args = args;
        }

        @Override
        public void run() {
            LOG.info("Thread begin.");
            HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
            try {
                highLevelClientTh = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
                LOG.info("Thread name: {}", Thread.currentThread().getName());
                bulk(highLevelClientTh, "example-huawei");
            } finally {
                if (highLevelClientTh != null) {
                    try {
                        highLevelClientTh.close();
                        LOG.info("Close the highLevelClient successful in thread : {}.",
                            Thread.currentThread().getName());
                    } catch (IOException e) {
                        LOG.error("Close the highLevelClient failed.", e);
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do bulk request.");
        int threadPoolSize = 5;
        int jobNumber = 5;
        ThreadPoolExecutor pool = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 60L, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(threadPoolSize));
        for (int i = 0; i < jobNumber; i++) {
            pool.execute(new SendRequestThread(args));
        }
        pool.shutdown();
    }
}
