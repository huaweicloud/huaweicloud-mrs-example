/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.bulk;

import com.huawei.fusioninsight.elasticsearch.example.LoadProperties;
import com.huawei.fusioninsight.elasticsearch.transport.client.ClientFactory;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
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
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * bulk example
 *
 * @since 2020-09-15
 */
public class BulkProcessorSample {
    private static final Logger LOG = LoggerFactory.getLogger(BulkProcessorSample.class);

    /**
     * 数据条数达到10000时进行刷新操作
     */
    private static int onceBulkMaxNum = 10000;

    /**
     * 数据量大小达到10M进行刷新操作
     */
    private static int onecBulkMaxSize = 10;

    /**
     * 需要入库的总条数
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
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            //在bulk请求之前进行调用
            @Override
            public void beforeBulk(long executionId, BulkRequest bulkRequest) {
                int numberOfActions = bulkRequest.numberOfActions();
                LOG.info("Executing bulk {} with {} requests.", executionId, numberOfActions);
            }
            //在bulkRequest请求成功后进行调用，在这一批次中可能存在失败的单个请求，需要调用bulkResponse.hasFailures()进行检查
            //建议：需要根据业务情况来处理这些失败的数据。
            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    BulkRequest errRequest = new BulkRequest();
                    LOG.error("Bulk failed, {}.", bulkResponse.buildFailureMessage());
                    int index = 0;
                    //获取失败的请求
                    for (BulkItemResponse bulkItemResponse : bulkResponse.getItems()) {
                        if (bulkItemResponse.isFailed()) {
                            errRequest.add(bulkRequest.requests().get(index));
                        }
                        index++;
                    }
                    LOG.error("Failed requests is: {}.", errRequest.requests().toString());
                } else {
                    LOG.info("Bulk {} completed in {} milliseconds.", executionId, bulkResponse.getTook().getMillis());
                }
            }
            //在整个bulkRequest批次失败时进行调用。
            //建议：需要根据业务情况来处理这些失败的数据。
            @Override
            public void afterBulk(long executionId, BulkRequest bulkRequest, Throwable throwable) {
                LOG.error("Failed to execute bulk.", throwable);
                LOG.error("Failed requests is: {}.", bulkRequest.requests().toString());
            }
        };

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer = transportClient::bulk;
        BulkProcessor bulkProcessor =
                BulkProcessor.builder(bulkConsumer, listener)
                        //10000条文档执行一次bulk
                        .setBulkActions(onceBulkMaxNum)
                        //当数据量达到10MB时执行一次bulk，文档数量或数据量其中一个达到阀值就会执行bulk请求。
                        .setBulkSize(new ByteSizeValue(onecBulkMaxSize, ByteSizeUnit.MB))
                        //设置并发请求数。值为0表示仅允许执行一个请求，值为1表示允许在累积新的批量请求时执行1个并发请求。
                        .setConcurrentRequests(concurrentRequestsNum)
                        //每10s执行一次bulk，建议不要将此值设置过小。
                        .setFlushInterval(TimeValue.timeValueSeconds(flushTime))
                        //当失败时设置一个自定义退避策略，以下策略表示重试3次，每次间隔1s，只有在返回状态码为429（请求太多）时才进行重试。
                        .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), maxRetry))
                        .build();

        LOG.info("Init bulkProcess successfully.");

        return bulkProcessor;
    }

    private void bulk() {
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
            bulkProcessorSample.bulk();
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
