/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.bulk;

import com.huawei.fusioninsight.elasticsearch.example.lowlevel.exist.IndexIsExist;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.index.CreateIndex;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import com.google.gson.Gson;

import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;

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
     * @param restClient low level 客户端
     * @param indexName 索引名
     */
    public static void bulkWithRouting(RestClient restClient, String indexName) {
        StringEntity entity;
        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<>();
        String str = "{ \"index\" : { \"_index\" : \"" + indexName + "\",";
        StringBuilder sb = new StringBuilder();
        int age;
        for (int i = 1; i <= NUM_OF_DOC; i++) {
            esMap.clear();
            esMap.put("user", "Linda" + i);
            age = ThreadLocalRandom.current().nextInt(18, 100);
            esMap.put("age", age);
            esMap.put("postDate", "2020-01-01");
            esMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
            esMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
            String strJson = gson.toJson(esMap);
            // 根据age进行routing
            sb.append(str).append("\"routing\":\"").append(age).append("\"}}").append(System.lineSeparator());
            sb.append(strJson).append(System.lineSeparator());
        }
        entity = new StringEntity(sb.toString(), ContentType.APPLICATION_JSON);
        entity.setContentEncoding("UTF-8");
        Response response;
        try {
            Request request = new Request("PUT", "_bulk");
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Bulk routing is successful.");
            } else {
                LOG.error("Bulk routing is failed.");
            }
        } catch (IOException e) {
            LOG.error("Bulk routing is failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        String indexName = "example-bulkrouting_lowlevel";
        RestClient restClient = null;
        try {
            HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
            restClient = hwRestClient.getRestClient();
            if (!IndexIsExist.indexIsExist(restClient, indexName)) {
                CreateIndex.createIndexWithShardNum(restClient, indexName);
            }
            bulkWithRouting(restClient, indexName);
        } finally {
            try {
                if (restClient != null) {
                    restClient.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close RestHighLevelClient", e);
            }
        }
    }
}
