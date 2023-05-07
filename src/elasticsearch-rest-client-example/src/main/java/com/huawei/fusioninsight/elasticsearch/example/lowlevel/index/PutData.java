/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.index;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import com.google.gson.Gson;

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
import java.util.HashMap;
import java.util.Map;

/**
 * 写入索引数据
 *
 * @since 2020-09-30
 */
public class PutData {
    private static final Logger LOG = LogManager.getLogger(PutData.class);

    /**
     * Write one document into the index
     */
    public static void putData(RestClient restClientTest, String index, String type, String id) {
        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<>();
        esMap.put("name", "Happy");
        esMap.put("author", "Alex Yang");
        esMap.put("pubinfo", "Beijing,China");
        esMap.put("pubtime", "2020-01-01");
        esMap.put("description",
            "Elasticsearch is a highly scalable open-source full-text search and analytics engine.");
        String jsonString = gson.toJson(esMap);
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response;

        try {
            Request request = new Request("POST", "/" + index + "/" + type + "/" + id);
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()
                || HttpStatus.SC_CREATED == response.getStatusLine().getStatusCode()) {
                LOG.info("PutData successful.");
            } else {
                LOG.error("PutData failed.");
            }
            LOG.info("PutData response entity is : {}.", EntityUtils.toString(response.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("PutData failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do putData request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            putData(restClient, "example-huawei1", "_doc", "1");
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
