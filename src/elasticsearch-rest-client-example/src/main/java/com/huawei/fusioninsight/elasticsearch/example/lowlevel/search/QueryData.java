/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.search;

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
 * 查询数据
 *
 * @since 2020-09-30
 */
public class QueryData {
    private static final Logger LOG = LogManager.getLogger(QueryData.class);

    /**
     * Query all data of one index.
     */
    public static void queryData(RestClient restClientTest, String index, String type, String id) {
        Response response;
        try {
            Request request = new Request("GET", "/" + index + "/" + type + "/" + id);
            request.addParameter("pretty", "true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("QueryData successful.");
            } else {
                LOG.error("QueryData failed.");
            }
            LOG.info("QueryData response entity is : {}.", EntityUtils.toString(response.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("QueryData failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do queryData request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            PutData.putData(restClient, "example-huawei1", "_doc", "1");
            queryData(restClient, "example-huawei1", "_doc", "1");
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
