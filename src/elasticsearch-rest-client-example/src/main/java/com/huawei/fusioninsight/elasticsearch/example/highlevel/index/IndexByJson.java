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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;

/**
 * 使用json格式字符串写入数据
 *
 * @since 2020-09-30
 */
public class IndexByJson {
    private static final Logger LOG = LogManager.getLogger(IndexByJson.class);

    /**
     * Create or update index by json
     */
    public static void indexByJson(RestHighLevelClient highLevelClient, String index, String id) {
        try {
            IndexRequest indexRequest = new IndexRequest(index).id(id);
            String jsonString =
                "{\"user\":\"kimchy1\",\"age\":\"100\",\"postDate\":\"2020-01-01\",\"message\":\"trying out Elasticsearch\",\"reason\":\"daily update\",\"innerObject1\":\"Object1\","
                    + "\"innerObject2\":\"Object2\",\"innerObject3\":\"Object3\",\"uid\":\"11\"}";
            indexRequest.source(jsonString, XContentType.JSON);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByJson response is {}.", indexResponse.toString());
        } catch (IOException e) {
            LOG.error("IndexByJson is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do indexByJson request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByJson(highLevelClient, "example-huawei", "1");
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
