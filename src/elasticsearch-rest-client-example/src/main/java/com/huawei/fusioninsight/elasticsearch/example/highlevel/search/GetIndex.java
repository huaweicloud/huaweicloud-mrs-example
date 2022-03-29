/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.search;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;

/**
 * 读取索引数据
 *
 * @since 2020-09-30
 */
public class GetIndex {
    private static final Logger LOG = LogManager.getLogger(GetIndex.class);

    /**
     * Get index information
     */
    private static void getIndex(RestHighLevelClient highLevelClient, String index, String id) {
        try {
            GetRequest getRequest = new GetRequest(index).id(id);
            String[] includes = new String[] {"message", "test*"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            getRequest.fetchSourceContext(fetchSourceContext);
            getRequest.storedFields("message");
            GetResponse getResponse = highLevelClient.get(getRequest, RequestOptions.DEFAULT);

            LOG.info("GetIndex response is {}.", getResponse.toString());
        } catch (IOException e) {
            LOG.error("GetIndex is failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do getIndex request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            getIndex(highLevelClient, "example-huawei", "1");
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
