/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.bulk;

import com.huawei.fusioninsight.elasticsearch.example.LoadProperties;
import com.huawei.fusioninsight.elasticsearch.transport.client.ClientFactory;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * bulk routing example
 *
 * @since 2020-09-15
 */
public class BulkRoutingSample {
    private static final Logger LOG = LogManager.getLogger(BulkRoutingSample.class);

    private static final long BULK_NUM = 100;

    private static PreBuiltHWTransportClient client;

    /**
     * 执行bulk请求，为每一个请求设置单独的路由，将相同age的文档放到相同分片
     *
     * @param client 客户端
     * @param indexName 索引名
     */
    public static void bulkWithRouting(PreBuiltHWTransportClient client, String indexName) {
        Map<String, Object> jsonMap = new HashMap<>();
        BulkRequest bulkRequest = new BulkRequest();
        int age;
        for (int i = 1; i <= BULK_NUM; i++) {
            jsonMap.clear();
            jsonMap.put("name", "Linda" + i);
            age = ThreadLocalRandom.current().nextInt(18, 100);
            jsonMap.put("age", age);
            jsonMap.put("height", 210);
            jsonMap.put("weight", 180);
            // 根据age来进行routing
            bulkRequest.add(new IndexRequest(indexName).source(jsonMap).routing(String.valueOf(age)));
        }
        BulkResponse response = client.bulk(bulkRequest).actionGet();
        if (response.hasFailures()) {
            LOG.info("Bulk routing is failed.");
        } else {
            LOG.info("Bulk routing is successful.");
        }
    }

    public static void main(String[] args) {
        String index = "example-bulkrouting_transport";
        try {
            ClientFactory.initConfiguration(LoadProperties.loadProperties(args));
            client = ClientFactory.getClient();
            bulkWithRouting(client, index);
        } catch (IOException e) {
            LOG.error("Exception is {}.", e.getMessage(), e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
