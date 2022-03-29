/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.cluster;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;

/**
 * 查询集群信息
 *
 * @since 2020-09-30
 */
public class QueryClusterInfo {
    private static final Logger LOG = LogManager.getLogger(QueryClusterInfo.class);

    /**
     * Query the cluster's information
     */
    public static void queryClusterInfo(RestClient restClientTest) {
        Response response;
        try {
            Request request = new Request("GET", "/_cluster/health");
            // 对返回结果进行处理，展示方式更直观
            request.addParameter("pretty", "true");
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("QueryClusterInfo successful.");
            } else {
                LOG.error("QueryClusterInfo failed.");
            }
            LOG.info("QueryClusterInfo response entity is : {}." , EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            LOG.error("QueryClusterInfo failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do queryClusterInfo request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            queryClusterInfo(restClient);
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
