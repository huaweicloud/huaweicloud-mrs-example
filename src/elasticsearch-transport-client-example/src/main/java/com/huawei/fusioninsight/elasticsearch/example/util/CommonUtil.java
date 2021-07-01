/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.index.IndexResponse;

/**
 * 通用工具类
 *
 * @since 2020-09-15
 */
public class CommonUtil {
    private static final Logger LOG = LogManager.getLogger(CommonUtil.class);

    /**
     * 异常处理
     *
     * @param exception ElasticsearchSecurityException
     */
    public static void handleException(ElasticsearchSecurityException exception) {
        LOG.error("Your permissions are incorrect," + exception.getMessage());
    }

    /**
     * 异常处理
     *
     * @param exception Exception
     */
    public static void handleException(Exception exception) {
        LOG.error("Your permissions are incorrect," + exception.getMessage());
    }

    /**
     * 打印索引信息
     *
     * @param response 索引响应
     */
    public static void printIndexInfo(IndexResponse response) {
        String indexName = response.getIndex();
        String id = response.getId();
        long version = response.getVersion();
        LOG.info("{},{},{}.", indexName, id, version);
    }
}
