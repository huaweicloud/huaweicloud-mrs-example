/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.search;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexDocs;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexSorting;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * range查询
 *
 * @since 2025-03-31
 */
public class RangeQuery {
    private static final Logger LOG = LogManager.getLogger(RangeQuery.class);

    private static void rangeQuery(RestHighLevelClient highLevelClient, String index) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age")
            .gte(30)
            .lte(50);
        sourceBuilder.query(rangeQueryBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("Range query response is {}.", response.toString());
        } catch (IOException e) {
            LOG.error("Failed to execute range query.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do match query request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            String indexName = "example-huawei";
            IndexDocs.indexDocs(highLevelClient, indexName);
            IndexSorting.refresh(highLevelClient, indexName);
            rangeQuery(highLevelClient, indexName);
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
