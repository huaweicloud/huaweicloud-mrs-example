/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.bulk;

import com.huawei.fusioninsight.elasticsearch.example.LoadProperties;
import com.huawei.fusioninsight.elasticsearch.transport.client.ClientFactory;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * bulk example
 *
 * @since 2020-09-15
 */
public class Bulk {
    private static final Logger LOG = LogManager.getLogger(Bulk.class);

    private static long threadCommitNum;

    private static final long PROCESS_RECORD_NUM = 10000;

    private static final long BULK_NUM = 1000;

    private static final int THREAD_NUM = 10;

    private static PreBuiltHWTransportClient client;

    /**
     * put data
     *
     * @param recordNum 单次bulk写入的文档数
     */
    private static void dataInput(long recordNum, String index, String type) {
        long circleCommit = recordNum / BULK_NUM;
        Map<String, Object> esJson = new HashMap<>();

        for (int j = 0; j < circleCommit; j++) {
            long startTime = System.currentTimeMillis();
            BulkRequestBuilder bulkRequest = client.prepare().prepareBulk();
            for (int i = 0; i < BULK_NUM; i++) {
                esJson.clear();
                esJson.put("id", "1");
                esJson.put("name", "Linda");
                esJson.put("sex", "man");
                esJson.put("age", 78);
                esJson.put("height", 210);
                esJson.put("weight", 180);
                bulkRequest.add(client.prepare().prepareIndex(index, type).setSource(esJson));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                LOG.warn("Batch indexing fail.");
            } else {
                LOG.info("Batch indexing success and put data time is {}.", (System.currentTimeMillis() - startTime));
            }
        }
    }

    private static void startThreadInput() {
        BlockingQueue blockingQueue = new LinkedBlockingDeque(THREAD_NUM);
        ThreadPoolExecutor pool = new ThreadPoolExecutor(THREAD_NUM, THREAD_NUM, 60L, TimeUnit.SECONDS, blockingQueue);
        MultipleThInputRun[] multipleThInputRuns = new MultipleThInputRun[THREAD_NUM];
        for (int i = 0; i < THREAD_NUM; i++) {
            multipleThInputRuns[i] = new MultipleThInputRun();
        }
        LOG.info("begin to execute bulk threads.");
        for (MultipleThInputRun multipleThInputRun : multipleThInputRuns) {
            if (multipleThInputRun != null) {
                pool.execute(multipleThInputRun);
            }
        }
        LOG.info("execute bulk threads successfully.");
    }

    public static void main(String[] args) {
        threadCommitNum = PROCESS_RECORD_NUM / THREAD_NUM;
        try {
            ClientFactory.initConfiguration(LoadProperties.loadProperties(args));
            client = ClientFactory.getClient();
            startThreadInput();
        } catch (IOException e) {
            LOG.error("Exception is {}.", e.getMessage(), e);
            System.exit(1);
        } finally {
            if (client != null) {
                client.close();
                LOG.info("Close the client successful in main.");
            }
        }
        System.exit(0);
    }

    static class MultipleThInputRun implements Runnable {
        @Override
        public void run() {
            Bulk.dataInput(Bulk.threadCommitNum, "example-indexname", "type");
        }
    }
}
