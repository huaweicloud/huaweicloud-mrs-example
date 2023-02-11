/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * bulkprocessor sample
 *
 * @since 2020-09-30
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
     * High level client
     */
    private RestHighLevelClient highLevelClient;

    /**
     * bulk processor
     */
    private BulkProcessor bulkProcessor;

    public BulkProcessorSample(RestHighLevelClient highLevelClient, BulkProcessor bulkProcessor) {
        this.highLevelClient = highLevelClient;
        this.bulkProcessor = bulkProcessor;
    }

    /**
     * 生成bulkProcessor
     *
     * @param highLevelClient 客户端
     * @return bulkprocessor实例
     */
    private static BulkProcessor getBulkProcessor(RestHighLevelClient highLevelClient) {
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

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer
            = (request, bulkListener) -> highLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        BulkProcessor bulkProcessor =
                BulkProcessor.builder(bulkConsumer, listener)
                        //10000条文档执行一次bulk
                        .setBulkActions(onceBulkMaxNum)
                        //当数据量达到10MB时执行一次bulk，文档数量或数据量其中一个达到阀值就会执行bulk请求
                        .setBulkSize(new ByteSizeValue(onecBulkMaxSize, ByteSizeUnit.MB))
                        //设置并发请求数。值为0表示仅允许执行一个请求，值为1表示允许在累积新的批量请求时执行1个并发请求。
                        .setConcurrentRequests(concurrentRequestsNum)
                        //每10s执行一次bulk，建议不要将此值设置过小。
                        .setFlushInterval(TimeValue.timeValueSeconds(flushTime))
                        //当失败时设置一个自定义退避策略，以下策略表示重试3次，每次间隔1s,只有在返回状态码为429（请求太多）时才进行重试。
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
            dataMap.put("textbody", "the test text");
            dataMap.put("title", "the title");
            bulkProcessor.add(new IndexRequest(indexName).source(dataMap));
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
     * high level 客户端，判断索引是否存在
     *
     * @return 索引是否存在
     */
    private static boolean isExistIndexForHighLevel(RestHighLevelClient highLevelClient) {
        GetIndexRequest isExistsRequest = new GetIndexRequest(indexName);
        try {
            return highLevelClient.indices().exists(isExistsRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Judge index exist {} failed", indexName, e);
        }
        return false;
    }

    /**
     * high level rest 客户端创建索引
     *
     * @return 是否创建成功
     */
    private static boolean createIndexForHighLevel(RestHighLevelClient highLevelClient) {
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
            indexRequest.settings(
                Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1));
            indexRequest.mapping(
                "{\"properties\": {\"date\": {\"type\": \"text\"},\"textbody\": {\"type\": \"text\"},\"title\": {\"type\": \"text\"}}}",
                XContentType.JSON);
            CreateIndexResponse response = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
            if (response.isAcknowledged() || response.isShardsAcknowledged()) {
                LOG.info("Create index {} successful by high level client.", indexName);
                return true;
            }
        } catch (IOException e) {
            LOG.error("Create index failed.", e);
        }
        return false;
    }

    public static void main(String[] args) {
        RestHighLevelClient highLevelClient = null;
        BulkProcessor bulkProcessor = null;
        BulkProcessorSample bulkProcessorSample = null;
        try {
            HwRestClient hwRestClient = new HwRestClient();
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            boolean isCreateSuccess = true;
            // 创建索引
            if (!isExistIndexForHighLevel(highLevelClient)) {
                isCreateSuccess = createIndexForHighLevel(highLevelClient);
            }
            if (!isCreateSuccess) {
                LOG.error("Create index {} failed.", indexName);
                return;
            }

            bulkProcessor = getBulkProcessor(highLevelClient);
            bulkProcessorSample = new BulkProcessorSample(highLevelClient, bulkProcessor);
            bulkProcessorSample.bulk();
        } finally {
            try {
                if (bulkProcessor != null) {
                    if (bulkProcessorSample != null) {
                        bulkProcessorSample.destroy();
                    }
                }
                if (highLevelClient != null) {
                    highLevelClient.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close RestHighLevelClient.", e);
                System.exit(1);
            }
        }
    }
}
