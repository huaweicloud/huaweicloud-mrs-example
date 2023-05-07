/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.bulk;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bulk example
 *
 * @since 2020-08-26
 */
public class Bulk {
    private static final Logger LOG = LogManager.getLogger(Bulk.class);

    /**
     * Bulk request can be used to to execute multiple index,update or delete
     * operations using a single request.
     */
    public static void bulk(RestHighLevelClient highLevelClient, String index) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            for (int i = 1; i <= 10; i++) {
                BulkRequest request = new BulkRequest();
                for (int j = 1; j <= 1000; j++) {
                    jsonMap.clear();
                    jsonMap.put("user", "Linda");
                    jsonMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                    jsonMap.put("postDate", "2020-01-01");
                    jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                    jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                    request.add(new IndexRequest(index).source(jsonMap));
                }
                BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);

                if (RestStatus.OK.equals((bulkResponse.status()))) {
                    LOG.info("Bulk is successful");
                } else {
                    LOG.info("Bulk is failed");
                }
            }
        } catch (IOException e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

    /**
     * Bulk with version
     * Bulk request can be used to to execute multiple index,update or delete
     * operations using a single request.
     */
    private static void bulkWithVersion(RestHighLevelClient highLevelClient, String indexName) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            for (int i = 1; i <= 100; i++) {
                BulkRequest request = new BulkRequest();
                for (int j = 1; j <= 1000; j++) {
                    jsonMap.clear();
                    jsonMap.put("user", "LindaVersion");
                    jsonMap.put("age", ThreadLocalRandom.current().nextInt(1, 100));
                    jsonMap.put("postDate", "2020-08-01");
                    jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(1, 1000));
                    jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(10, 200));
                    request.add(
                        new UpdateRequest(indexName, String.valueOf(ThreadLocalRandom.current().nextInt(1, 100))).doc(
                            XContentType.JSON, "update", new Date())
                            .setIfSeqNo(1L)
                            .setIfPrimaryTerm(1L)
                            .upsert(jsonMap, XContentType.JSON));
                }
                BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);

                if (RestStatus.OK.equals((bulkResponse.status()))) {
                    LOG.info("Bulk is successful");
                } else {
                    LOG.info("Bulk is failed");
                }
            }
        } catch (IOException e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

    /**
     * high level rest 客户端创建索引
     *
     * @param highLevelClient high level rest 客户端
     * @return 是否创建成功
     */
    private static boolean createIndexForHighLevel(RestHighLevelClient highLevelClient, String indexName) {
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
            indexRequest.settings(
                Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1));
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
        LOG.info("Start to do bulk request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            // 创建索引
            String indexName = "example-indexname";
            boolean isCreateSuccess = true;
            if (!HwRestClientUtils.isExistIndexForHighLevel(highLevelClient, indexName)) {
                isCreateSuccess = createIndexForHighLevel(highLevelClient, indexName);
            }

            if (isCreateSuccess) {
                bulk(highLevelClient, indexName);
            } else {
                LOG.error("Create index {} failed.", indexName);
            }

            // 创建索引
            String indexNameWithVersion = "example-index_with_version";
            isCreateSuccess = true;
            if (!HwRestClientUtils.isExistIndexForHighLevel(highLevelClient, indexNameWithVersion)) {
                isCreateSuccess = createIndexForHighLevel(highLevelClient, indexNameWithVersion);
            }
            if (isCreateSuccess) {
                bulkWithVersion(highLevelClient, indexNameWithVersion);
            } else {
                LOG.error("Create index {} failed.", indexNameWithVersion);
                return;
            }
        } finally {
            try {
                if (highLevelClient != null) {
                    highLevelClient.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close RestHighLevelClient.", e);
            }
        }
    }
}
