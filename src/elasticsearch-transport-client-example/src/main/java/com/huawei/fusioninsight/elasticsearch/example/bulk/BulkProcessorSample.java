/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.bulk;

import com.huawei.fusioninsight.elasticsearch.example.LoadProperties;
import com.huawei.fusioninsight.elasticsearch.transport.client.ClientFactory;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class BulkProcessorSample {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProcessorSample.class);

    /**
     * 数据条数达到1000时进行刷新操作
     */
    private static int onceBulkMaxNum = 1000;

    /**
     * 多线程运行的线程数
     */
    private static int threadNum = 5;

    /**
     * 数据量大小达到5M进行刷新操作
     */
    private static int onecBulkMaxSize = 5;

    /**
     * 单个线程需要入库的总条数
     */
    private static int totalNumberForThread = 20000;

    /**
     * 设置允许执行的并发请求数
     */
    private static int concurrentRequestsNum = 5;

    /**
     * 设置刷新间隔时间，如果超过刷新时间则BulkRequest挂起
     */
    private static int flushTime = 10;

    /**
     * 设置刷新间隔时间，如果超过刷新时间则BulkRequest挂起
     */
    private static int maxRetry = 3;

    /**
     * 索引名
     */
    private static String indexName = "example-bulkindex";

    /**
     * 索引类型
     */
    private static String indexType = "blog";

    /**
     * transport client
     */
    private PreBuiltHWTransportClient transportClient;

    /**
     * bulk processor
     */
    private BulkProcessor bulkProcessor;

    /**
     * 存放任务的队列
     */
    private BlockingQueue blockingQueue = new LinkedBlockingDeque(threadNum);

    /**
     * 线程池
     */
    private ThreadPoolExecutor threadPool =
            new ThreadPoolExecutor(threadNum, threadNum, 60L, TimeUnit.SECONDS, blockingQueue);

    /**
     * 默认多线程运行
     */
    private boolean isSingleThread = false;

    public BulkProcessorSample(PreBuiltHWTransportClient transportClient, BulkProcessor bulkProcessor) {
        this.transportClient = transportClient;
        this.bulkProcessor = bulkProcessor;
    }

    /**
     * 生成bulkProcessor
     *
     * @param transportClient transport客户端
     * @return BulkProcessor实例
     */
    private static BulkProcessor getBulkProcessor(PreBuiltHWTransportClient transportClient) {
        BulkProcessor.Listener listener =
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                        int numberOfActions = bulkRequest.numberOfActions();
                        LOG.info("Executing bulk {} with {} requests.", executionId, numberOfActions);
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                        if (bulkResponse.hasFailures()) {
                            LOG.warn("Bulk {} executed with failures.", executionId);
                        } else {
                            LOG.info(
                                    "Bulk {} completed in {} milliseconds.",
                                    executionId,
                                    bulkResponse.getTook().getMillis());
                        }
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                        LOG.error("Failed to execute bulk.", throwable);
                    }
                };
        BulkProcessor bulkProcessor =
                BulkProcessor.builder(transportClient, listener)
                        .setBulkActions(onceBulkMaxNum)
                        .setBulkSize(new ByteSizeValue(onecBulkMaxSize, ByteSizeUnit.MB))
                        .setConcurrentRequests(concurrentRequestsNum)
                        .setFlushInterval(TimeValue.timeValueSeconds(flushTime))
                        .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), maxRetry))
                        .build();

        LOG.info("Init bulkProcess successfully.");

        return bulkProcessor;
    }

    /**
     * 多线程运行入库
     */
    private class ConsumerTask implements Runnable {
        @Override
        public void run() {
            singleThreadBulk();
        }
    }

    /**
     * 单线程样例方法
     */
    private void singleThreadBulk() {
        // 单线程
        int bulkTime = 0;
        while (bulkTime++ < totalNumberForThread) {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("date", "2019/12/9");
            dataMap.put("text", "the test text");
            dataMap.put("title", "the title");
            bulkProcessor.add(
                    transportClient.prepare().prepareIndex(indexName, indexType).setSource(dataMap).request());
            // 安全模式下，transport客户端在多线程中直接new IndexRequest，会出现认证错误
            // 不能直接new IndexRequest: bulkProcessor.add(new IndexRequest(indexName, indexType).source(dataMap));
        }
        LOG.info("This thead bulks successfully, the thread name is {}.", Thread.currentThread().getName());
    }

    /**
     * 多线程样例方法
     */
    private void multiThreadBulk() {
        for (int i = 0; i < threadNum; i++) {
            threadPool.execute(new ConsumerTask());
        }
    }

    /**
     * 关闭线程池
     */
    private void shutDownThreadPool() {
        try {
            threadPool.shutdown();
            while (true) {
                if (threadPool.isTerminated()) {
                    LOG.info("All bulkdata threads have ran end.");
                    break;
                }
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            LOG.error("Close bulkThreadPool failed.");
        }
    }

    private void destroy() {
        try {
            // 执行关闭方法会把bulk剩余的数据都写入ES再执行关闭
            bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Failed to close bulkProcessor", e);
        }
        LOG.info("BulkProcessor closed.");
    }

    /**
     * 主样例方法
     *
     * @param args 运行参数
     */
    public static final void main(String[] args) {
        PreBuiltHWTransportClient transportClient = null;
        BulkProcessor bulkProcessor = null;
        BulkProcessorSample bulkProcessorSample = null;
        try {
            ClientFactory.initConfiguration(LoadProperties.loadProperties(args));
            transportClient = ClientFactory.getClient();
            bulkProcessor = getBulkProcessor(transportClient);
            bulkProcessorSample = new BulkProcessorSample(transportClient, bulkProcessor);

            if (bulkProcessorSample.isSingleThread) {
                // 单线程样例
                bulkProcessorSample.singleThreadBulk();
            } else {
                // 多线程样例
                bulkProcessorSample.multiThreadBulk();
                bulkProcessorSample.shutDownThreadPool();
            }
        } catch (IOException e) {
            LOG.error("Init bulkProcessorSample false.", e);
            System.exit(1);
        } finally {
            if (bulkProcessor != null) {
                bulkProcessorSample.destroy();
            }
            if (transportClient != null) {
                transportClient.close();
            }
        }
    }
}
