/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.index;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 通过Map写入数据
 *
 * @since 2020-09-30
 */
public class IndexByMap {
    private static final Logger LOG = LogManager.getLogger(IndexByMap.class);

    /**
     * Create or update index by map
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index Index name
     * @param id Document id
     */
    public static void indexByMap(RestHighLevelClient highLevelClient, String index, String id) {
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("user", "kimchy2");
            dataMap.put("age", "200");
            dataMap.put("postDate", new Date());
            dataMap.put("message", "trying out Elasticsearch");
            dataMap.put("reason", "daily update");
            dataMap.put("innerObject1", "Object1");
            dataMap.put("innerObject2", "Object2");
            dataMap.put("innerObject3", "Object3");
            dataMap.put("uid", "22");
            IndexRequest indexRequest = new IndexRequest(index).id(id).source(dataMap);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByMap response is {}.", indexResponse.toString());
        } catch (Exception e) {
            LOG.error("IndexByMap is failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do indexByMap request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByMap(highLevelClient, "example-huawei", "1");
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
