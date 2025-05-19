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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * bool组合查询
 *
 * @since 2025-03-31
 */
public class BoolQuery {
    private static final Logger LOG = LogManager.getLogger(BoolQuery.class);

    private static void boolQuery(RestHighLevelClient highLevelClient, String index) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 构建 Bool 查询
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
            .should(QueryBuilders.multiMatchQuery("python", "title", "content"))
            .must(QueryBuilders.termQuery("user", "zhang"));
        sourceBuilder.query(boolQuery);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("Bool query response is {}.", response.toString());
        } catch (IOException e) {
            LOG.error("Failed to execute bool query.", e);
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
            boolQuery(highLevelClient, indexName);
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
