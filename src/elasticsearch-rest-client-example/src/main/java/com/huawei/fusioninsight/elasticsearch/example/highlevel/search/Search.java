/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.search;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByJson;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 查询索引
 *
 * @since 2020-09-30
 */
public class Search {
    private static final Logger LOG = LogManager.getLogger(Search.class);

    /**
     * Search some information in index
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index Index name
     */
    public static void search(RestHighLevelClient highLevelClient, String index) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery("user", "kimchy1"));

            searchSourceBuilder.sort(new FieldSortBuilder("_doc").order(SortOrder.ASC));

            // Source filter
            String[] includeFields = new String[] {"message", "user", "innerObject*"};
            String[] excludeFields = new String[] {"postDate"};
            searchSourceBuilder.fetchSource(includeFields, excludeFields);
            // Control which fields get included or
            // excluded
            // Request Highlighting
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
            // Create a field highlighter for
            // the user field
            highlightBuilder.field(highlightUser);
            searchSourceBuilder.highlighter(highlightBuilder);
            searchSourceBuilder.from(0);
            searchSourceBuilder.size(2);
            searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            LOG.info("Search response is {}.", searchResponse.toString());
        } catch (IOException e) {
            LOG.error("Search is failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do search request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            IndexByJson.indexByJson(highLevelClient, "example-huawei", "1");
            search(highLevelClient, "example-huawei");
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
