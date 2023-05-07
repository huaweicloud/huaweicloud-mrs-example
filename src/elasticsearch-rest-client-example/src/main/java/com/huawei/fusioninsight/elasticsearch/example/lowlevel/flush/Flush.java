/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.flush;

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
 * 将数据刷到存储
 *
 * @since 2020-09-30
 */
public class Flush {
    private static final Logger LOG = LogManager.getLogger(Flush.class);

    /**
     * Flush one index data to storage and clearing the internal transaction log
     */
    public static void flushOneIndex(RestClient restClientTest, String index) {
        Response flushRsp;
        try {
            Request request = new Request("POST", "/" + index + "/_flush");
            request.addParameter("pretty", "true");
            flushRsp = restClientTest.performRequest(request);
            LOG.info(EntityUtils.toString(flushRsp.getEntity()));

            if (HttpStatus.SC_OK == flushRsp.getStatusLine().getStatusCode()) {
                LOG.info("Flush one index successful.");
            } else {
                LOG.error("Flush one index failed.");
            }
            LOG.info("Flush one index response entity is : {}.", EntityUtils.toString(flushRsp.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("Flush one index failed, exception occurred.", e);
        }
    }

    /**
     * Flush all indexes data to storage and clearing the internal transaction log
     * The user who performs this operation must belong to the "supergroup" group.
     */
    private static void flushAllIndexes(RestClient restClientTest) {
        Response flushRsp;
        try {
            Request request = new Request("POST", "/_all/_flush");
            request.addParameter("pretty", "true");
            flushRsp = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == flushRsp.getStatusLine().getStatusCode()) {
                LOG.info("Flush all indexes successful.");
            } else {
                LOG.error("Flush all indexes failed.");
            }
            LOG.info("Flush all indexes response entity is : {}.", EntityUtils.toString(flushRsp.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("Flush all indexes failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do flush request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            PutData.putData(restClient, "example-huawei1", "type1", "1");
            flushOneIndex(restClient, "example-huawei1");
            flushAllIndexes(restClient);
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
