/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.delete;

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
 * 删除索引中部分文档
 *
 * @since 2020-09-30
 */
public class DeleteSomeDocumentsInIndex {
    private static final Logger LOG = LogManager.getLogger(DeleteSomeDocumentsInIndex.class);

    /**
     * Delete some documents by query in one index
     */
    public static void deleteSomeDocumentsInIndex(RestClient restClientTest, String index, String field, String value) {
        String jsonString = "{\n" + "  \"query\": {\n" + "    \"match\": { \"" + field + "\":\"" + value + "\"}\n"
            + "  }\n" + "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response;
        try {
            Request request = new Request("POST", "/" + index + "/_delete_by_query");
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("DeleteSomeDocumentsInIndex successful.");
            } else {
                LOG.error("DeleteSomeDocumentsInIndex failed.");
            }
            LOG.info("DeleteSomeDocumentsInIndex response entity is : {}.", EntityUtils.toString(response.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("DeleteSomeDocumentsInIndex failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do deleteAllDocumentsInIndex request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            deleteSomeDocumentsInIndex(restClient, "example-huawei1", "pubinfo", "Beijing");
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
