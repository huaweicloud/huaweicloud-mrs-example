/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.index;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.delete.DeleteIndex;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.bulk.Bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 索引预排序样例
 *
 * @since 2023-01-10
 */
public class IndexSorting {
    private static final Logger LOG = LogManager.getLogger(IndexSorting.class);

    private static void createIndexWithSorting(RestHighLevelClient highLevelClient, String index) {
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest(index);
            indexRequest.settings(
                Settings.builder().put("index.number_of_shards", 3)
                    .put("index.number_of_replicas", 1)
                    .putList("index.sort.field", "height", "weight")
                    .putList("index.sort.order", SortOrder.DESC.toString(), SortOrder.ASC.toString()));
            indexRequest.mapping("{\"properties\": {\"height\": {\"type\": \"float\"},"
                + "\"weight\": {\"type\": \"float\"}}}", XContentType.JSON);
            CreateIndexResponse response = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
            if (response.isAcknowledged() || response.isShardsAcknowledged()) {
                LOG.info("Create index with sorting successfully.");
            }
        } catch (IOException e) {
            LOG.error("Failed to create index with sorting, exception occurred.", e);
        }
    }

    public static void refresh(RestHighLevelClient highLevelClient, String index) {
        try {
            RefreshRequest refRequest = new RefreshRequest(index);
            highLevelClient.indices().refresh(refRequest, RequestOptions.DEFAULT);
            LOG.info("Refresh sorting index successfully.");
        } catch (IOException e) {
            LOG.error("Failed to refresh sorting index, exception occurred.", e);
        }
    }

    private static void queryData(RestHighLevelClient highLevelClient, String index) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.sort(new FieldSortBuilder("height").order(SortOrder.DESC));
            searchSourceBuilder.sort(new FieldSortBuilder("weight").order(SortOrder.ASC));
            // Source filter
            String[] includeFields = new String[]{"weight", "height", "age"};
            String[] excludeFields = new String[]{};
            searchSourceBuilder.fetchSource(includeFields, excludeFields);
            // Control which fields get included or excluded
            // Request Highlighting
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("height");
            highlightBuilder.field(highlightUser);
            searchSourceBuilder.highlighter(highlightBuilder);
            searchSourceBuilder.timeout(new TimeValue(1, TimeUnit.SECONDS));
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("Search response is {}.", searchResponse.toString());
        } catch (IOException e) {
            LOG.error("Failed to query sorting index, exception occurred.", e);
        }
    }

    /**
     * index sorting sample
     *
     * @param highLevelClient high level 客户端
     */
    public static void doIndexSorting(RestHighLevelClient highLevelClient) {
        String indexName = "example-sort";
        if (HwRestClientUtils.isExistIndexForHighLevel(highLevelClient, indexName)) {
            DeleteIndex.deleteIndex(highLevelClient, indexName);
        }
        createIndexWithSorting(highLevelClient, indexName);
        Bulk.bulk(highLevelClient, indexName);
        refresh(highLevelClient, indexName);
        queryData(highLevelClient, indexName);
    }

    private static void closeClient(RestHighLevelClient highLevelClient) {
        try {
            if (highLevelClient != null) {
                highLevelClient.close();
            }
        } catch (IOException e) {
            LOG.error("Failed to close RestHighLevelClient.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do index sorting.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            doIndexSorting(highLevelClient);
        } finally {
            closeClient(highLevelClient);
        }
    }
}
