/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.cluster;

import com.huawei.fusioninsight.elasticsearch.example.util.CommonUtil;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;

/**
 * cluster example
 *
 * @since 2020-09-15
 */
public class ClusterSample {
    private static final Logger LOG = LogManager.getLogger(ClusterSample.class);

    /**
     * 获取集群健康状态
     *
     * @param client 客户端
     */
    public static void clusterHealth(PreBuiltHWTransportClient client) {
        ClusterHealthResponse healths;
        try {
            healths = client.prepare().admin().cluster().prepareHealth().get();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        LOG.info(healths.toString());
    }
}
