/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.update;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByJson;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;
import java.util.Date;

/**
 * 更新索引
 *
 * @since 2020-09-30
 */
public class Update {
    private static final Logger LOG = LogManager.getLogger(Update.class);

    /**
     * Update index
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index Index name
     * @param id Document id
     */
    public static void update(RestHighLevelClient highLevelClient, String index, String id) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            { // update information
                builder.field("postDate", new Date());
                builder.field("reason", "update again");
            }
            builder.endObject();
            UpdateRequest request = new UpdateRequest(index, id).doc(builder);
            UpdateResponse updateResponse = highLevelClient.update(request, RequestOptions.DEFAULT);

            LOG.info("Update response is {}.", updateResponse.toString());
        } catch (IOException e) {
            LOG.error("Update is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do update request !");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            IndexByJson.indexByJson(highLevelClient, "example-huawei", "1");
            update(highLevelClient, "example-huawei", "1");
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
