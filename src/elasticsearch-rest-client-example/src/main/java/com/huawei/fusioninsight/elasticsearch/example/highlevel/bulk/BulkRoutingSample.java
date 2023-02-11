/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.bulk;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * bulk routing example
 *
 * @since 2020-09-11
 */
public class BulkRoutingSample {
    private static final Logger LOG = LogManager.getLogger(BulkRoutingSample.class);

    private static final int NUM_OF_DOC = 100;

    /**
     * 使用bulk 写入数据，同时指定routing
     *
     * @param highLevelClient high level 客户端
     * @param indexName 索引名
     */
    private static void bulkWithRouting(RestHighLevelClient highLevelClient, String indexName) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            BulkRequest request = new BulkRequest();
            int age;
            for (int i = 1; i <= NUM_OF_DOC; i++) {
                jsonMap.clear();
                jsonMap.put("user", "Linda" + i);
                age = ThreadLocalRandom.current().nextInt(18, 100);
                jsonMap.put("age", age);
                jsonMap.put("postDate", "2020-01-01");
                jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                // 根据age进行routing，将相同age的数据放在相同分片
                request.add(new IndexRequest(indexName).source(jsonMap).routing(String.valueOf(age)));
            }
            BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);
            if (RestStatus.OK.equals(bulkResponse.status())) {
                LOG.info("Bulk routing is successful.");
            } else {
                LOG.info("Bulk routing is failed.");
            }
        } catch (IOException e) {
            LOG.error("Bulk routing is failed, exception occurred.", e);
        }
    }

    /**
     * high level 客户端，判断索引是否存在
     *
     * @param highLevelClient high level 客户端
     * @return 索引是否存在
     */
    private static boolean isExistIndexForHighLevel(RestHighLevelClient highLevelClient, String indexName) {
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
     * @param highLevelClient high level rest 客户端
     */
    private static void createIndexForHighLevel(RestHighLevelClient highLevelClient, String indexName) {
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
            indexRequest.settings(
                Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1));
            CreateIndexResponse response = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
            if (response.isAcknowledged() || response.isShardsAcknowledged()) {
                LOG.info("Create index {} successful by high level client.", indexName);
            }
        } catch (IOException e) {
            LOG.error("Create index failed.", e);
        }
    }

    public static void main(String[] args) {
        String indexName = "example-bulkrouting_highlevel";
        RestHighLevelClient highLevelClient = null;
        try {
            HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            if (!isExistIndexForHighLevel(highLevelClient, indexName)) {
                createIndexForHighLevel(highLevelClient, indexName);
            }
            bulkWithRouting(highLevelClient, indexName);

        } finally {
            try {
                if (highLevelClient != null) {
                    highLevelClient.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close RestHighLevelClient", e);
            }
        }
    }
}
