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
 * 删除索引中所有文档
 *
 * @since 2020-09-30
 */
public class DeleteAllDocumentsInIndex {
    private static final Logger LOG = LogManager.getLogger(DeleteAllDocumentsInIndex.class);

    /**
     * Delete all documents by query in one index
     */
    private static void deleteAllDocumentsInIndex(RestClient restClientTest, String index) {
        String jsonString = "{\n" + "  \"query\": {\n" + "    \"match_all\": {}\n" + "  }\n" + "}";
        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        Response response;
        try {
            Request request = new Request("POST", "/" + index + "/_delete_by_query");
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("DeleteAllDocumentsInIndex successful.");
            } else {
                LOG.error("DeleteAllDocumentsInIndex failed.");
            }
            LOG.info("DeleteAllDocumentsInIndex response entity is : {}.", EntityUtils.toString(response.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("DeleteAllDocumentsInIndex failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do deleteAllDocumentsInIndex request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            deleteAllDocumentsInIndex(restClient, "example-huawei1");
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
