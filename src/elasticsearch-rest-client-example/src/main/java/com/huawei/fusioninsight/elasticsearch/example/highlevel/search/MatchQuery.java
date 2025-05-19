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
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * match&multi_match查询
 *
 * @since 2025-03-31
 */
public class MatchQuery {
    private static final Logger LOG = LogManager.getLogger(MatchQuery.class);

    /**
     * Match query in an index
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index           Index name
     */
    private static void matchQuery(RestHighLevelClient highLevelClient, String index) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("title", "python");
        sourceBuilder.query(matchQueryBuilder);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("Match query response is {}.", searchResponse.toString());
        } catch (IOException e) {
            LOG.error("Failed to execute multi match.", e);
        }
    }

    /**
     * Multi-match query in an index
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index           Index name
     */
    private static void multiMatchQuery(RestHighLevelClient highLevelClient, String index) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        MultiMatchQueryBuilder query = QueryBuilders.multiMatchQuery("python", "title", "content");
        sourceBuilder.query(query);
        searchRequest.source(sourceBuilder);
        try {
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("Multi-match query response is {}.", searchResponse.toString());
        } catch (IOException e) {
            LOG.error("Failed to execute multi match query.", e);
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
            matchQuery(highLevelClient, indexName);
            multiMatchQuery(highLevelClient, indexName);
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
