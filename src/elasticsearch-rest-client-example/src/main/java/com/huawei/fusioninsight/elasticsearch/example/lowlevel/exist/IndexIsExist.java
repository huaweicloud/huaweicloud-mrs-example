/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.exist;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;

/**
 * 判断索引是否存在
 *
 * @since 2020-09-30
 */
public class IndexIsExist {
    private static final Logger LOG = LogManager.getLogger(IndexIsExist.class);

    /**
     * Check the existence of the index or not.
     */
    public static boolean indexIsExist(RestClient restClientTest, String index) {
        Response response;
        try {
            Request request = new Request("HEAD", "/" + index);
            request.addParameter("pretty", "true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Check index successful,index is exist : {}.", index);
                return true;
            }
            if (HttpStatus.SC_NOT_FOUND == response.getStatusLine().getStatusCode()) {
                LOG.info("Index is not exist : {}.", index);
                return false;
            }
        } catch (IOException e) {
            LOG.error("Check index failed, exception occurred.", e);
        }

        return false;
    }

    public static void main(String[] args) {
        LOG.info("Start to do indexIsExist request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            indexIsExist(restClient, "example-huawei2");
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
