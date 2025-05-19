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
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * wildcard查询
 *
 * @since 2025-03-31
 */
public class WildcardQuery {
    private static final Logger LOG = LogManager.getLogger(WildcardQuery.class);

    private static void wildcardQuery(RestHighLevelClient highLevelClient, String index) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        WildcardQueryBuilder query = QueryBuilders.wildcardQuery("user.keyword", "z*g");
        sourceBuilder.query(query);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("Wildcard query response is {}.", searchResponse.toString());
        } catch (IOException e) {
            LOG.error("Failed to execute Wildcard query.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do wildcard query request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            String indexName = "example-huawei";
            IndexDocs.indexDocs(highLevelClient, indexName);
            IndexSorting.refresh(highLevelClient, indexName);
            wildcardQuery(highLevelClient, indexName);
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
