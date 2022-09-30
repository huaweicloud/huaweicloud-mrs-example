/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.index;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;

/**
 * 创建索引
 *
 * @since 2020-09-30
 */
public class CreateIndex {
    private static final Logger LOG = LogManager.getLogger(CreateIndex.class);

    /**
     * Create one index with customized shard number and replica number.
     */
    public static void createIndexWithShardNum(RestClient restClientTest, String index) {
        Response rsp;
        int shardNum = 3;
        int replicaNum = 1;
        String jsonString = "{" + "\"settings\":{" + "\"number_of_shards\":\"" + shardNum + "\","
            + "\"number_of_replicas\":\"" + replicaNum + "\"" + "}}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        try {
            Request request = new Request("PUT", "/" + index);
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            rsp = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == rsp.getStatusLine().getStatusCode()) {
                LOG.info("CreateIndexWithShardNum successful.");
            } else {
                LOG.error("CreateIndexWithShardNum failed.");
            }
            LOG.info("CreateIndexWithShardNum response entity is : {}.", EntityUtils.toString(rsp.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("CreateIndexWithShardNum failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do createIndex request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            createIndexWithShardNum(restClient, "example-huawei1");
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                    LOG.info("Close the client successful.");
                } catch (IOException e1) {
                    LOG.error("Close the client failed.", e1);
                }
            }
        }
    }
}
