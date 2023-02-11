/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.cluster;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
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
     * Get cluster information
     */
    public static void queryClusterInfo(RestHighLevelClient highLevelClient) {
        try {
            MainResponse response = highLevelClient.info(RequestOptions.DEFAULT);
            String clusterName = response.getClusterName();
            LOG.info("ClusterName:[{}], clusterUuid:[{}], nodeName:[{}], version:[{}].", clusterName,
                response.getClusterUuid(), response.getNodeName(), response.getVersion().toString());
        } catch (IOException e) {
            LOG.error("QueryClusterInfo is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do queryClusterInfo test!");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            queryClusterInfo(highLevelClient);
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
