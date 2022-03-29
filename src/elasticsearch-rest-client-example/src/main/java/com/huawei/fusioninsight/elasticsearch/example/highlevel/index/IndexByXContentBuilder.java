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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;

/**
 * 通过XContentBuilder格式写入数据
 *
 * @since 2020-09-30
 */
public class IndexByXContentBuilder {
    private static final Logger LOG = LogManager.getLogger(IndexByXContentBuilder.class);

    /**
     * Create or update index by XContentBuilder
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index Index name
     * @param id Document id
     */
    public static void indexByXContentBuilder(RestHighLevelClient highLevelClient, String index, String id) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy3");
                builder.field("age", "300");
                builder.field("postDate", "2020-01-01");
                builder.field("message", "trying out Elasticsearch");
                builder.field("reason", "daily update");
                builder.field("innerObject1", "Object1");
                builder.field("innerObject2", "Object2");
                builder.field("innerObject3", "Object3");
                builder.field("uid", "33");
            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(index).id(id).source(builder);
            IndexResponse indexResponse = highLevelClient.index(indexRequest, RequestOptions.DEFAULT);

            LOG.info("IndexByXContentBuilder response is {}.", indexResponse.toString());
        } catch (IOException e) {
            LOG.error("IndexByXContentBuilder is failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do indexByXContentBuilder request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            indexByXContentBuilder(highLevelClient, "example-huawei", "1");
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
