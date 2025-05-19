/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.search;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByJson;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * 通过游标搜索
 *
 * @since 2020-09-30
 */
public class SearchScroll {
    private static final Logger LOG = LogManager.getLogger(SearchScroll.class);

    /**
     * Send a search scroll request
     *
     * @param highLevelClient Elasticsearch high level client
     * @param index Index name
     */
    public static String searchScroll(RestHighLevelClient highLevelClient, String index) {
        String scrollId;
        try {
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchQuery("title", "Elasticsearch"));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            LOG.info("SearchHits is {}", searchResponse.toString());

            while (searchHits != null && searchHits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = highLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
                LOG.info("SearchHits is {}", searchResponse.toString());
            }
        } catch (IOException e) {
            LOG.error("SearchScroll is failed,exception occurred.", e);
            scrollId = null;
        }
        return scrollId;
    }

    /**
     * Clear a search scroll
     *
     * @param highLevelClient Elasticsearch high level client
     * @param scrollId Scroll id
     */
    public static void clearScroll(RestHighLevelClient highLevelClient, String scrollId) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse;
        try {
            clearScrollResponse = highLevelClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            if (clearScrollResponse.isSucceeded()) {
                LOG.info("ClearScroll is successful.");
            } else {
                LOG.error("ClearScroll is failed.");
            }
        } catch (IOException e) {
            LOG.error("ClearScroll is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do searchScroll request.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            IndexByJson.indexByJson(highLevelClient, "example-huawei", "1");
            String scrollId = searchScroll(highLevelClient, "example-huawei");
            clearScroll(highLevelClient, scrollId);
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
