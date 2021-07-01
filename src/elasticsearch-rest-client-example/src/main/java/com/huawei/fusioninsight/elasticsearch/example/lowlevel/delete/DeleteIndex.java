/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.delete;

import com.huawei.fusioninsight.elasticsearch.example.lowlevel.index.PutData;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;

/**
 * 删除索引
 *
 * @since 2020-09-30
 */
public class DeleteIndex {
    private static final Logger LOG = LogManager.getLogger(DeleteIndex.class);

    /**
     * Delete one index
     */
    public static void deleteIndex(RestClient restClientTest, String index) {
        Response response;
        try {
            Request request = new Request("DELETE", "/" + index + "?&pretty=true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Delete index successful.");
            } else {
                LOG.error("Delete index failed.");
            }
            LOG.info("Delete index response entity is : {}.", EntityUtils.toString(response.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("Delete index failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do deleteIndex request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            PutData.putData(restClient, "example-huawei1", "_doc", "1");
            deleteIndex(restClient, "example-huawei1");
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
