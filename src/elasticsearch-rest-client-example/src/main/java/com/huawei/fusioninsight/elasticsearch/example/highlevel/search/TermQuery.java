/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.search;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByJson;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByMap;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByXContentBuilder;
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
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Arrays;

/**
 * term&terms查询
 *
 * @since 2025-03-31
 */
public class TermQuery {
    private static final Logger LOG = LogManager.getLogger(TermQuery.class);

    /**
     * Term query in index
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index Index name
     */
    public static void termQuery(RestHighLevelClient highLevelClient, String index) {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy1"));
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            LOG.info("Term query response is {}.", searchResponse.toString());
        } catch (IOException e) {
            LOG.error("Term query is failed, exception occurred.", e);
        }
    }

    /**
     * Terms query in index
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index Index name
     */
    public static void termsQuery(RestHighLevelClient highLevelClient, String index) {
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(QueryBuilders.termsQuery("user", Arrays.asList("kimchy1", "kimchy2")));
            searchRequest.source(sourceBuilder);
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            LOG.info("Terms query response is {}.", searchResponse.toString());
        } catch (IOException e) {
            LOG.error("Terms query is failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do term query request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            String indexName = "example-huawei";
            IndexByJson.indexByJson(highLevelClient, indexName, "1");
            IndexByMap.indexByMap(highLevelClient, indexName, "2");
            IndexByXContentBuilder.indexByXContentBuilder(highLevelClient, indexName, "3");
            IndexSorting.refresh(highLevelClient, indexName);
            termQuery(highLevelClient, indexName);
            termsQuery(highLevelClient, indexName);
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