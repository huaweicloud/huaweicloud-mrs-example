/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.delete;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByJson;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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
     * Delete the index
     */
    public static void deleteIndex(RestHighLevelClient highLevelClient, String index) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteResponse = highLevelClient.indices().delete(request, RequestOptions.DEFAULT);
            if (deleteResponse.isAcknowledged()) {
                LOG.info("Delete index is successful.");
            } else {
                LOG.info("Delete index is failed.");
            }
        } catch (IOException e) {
            LOG.error("Delete index : {} is failed, exception occurred.", index, e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do deleteIndex request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            IndexByJson.indexByJson(highLevelClient, "example-huawei", "1");
            deleteIndex(highLevelClient, "example-huawei");
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
