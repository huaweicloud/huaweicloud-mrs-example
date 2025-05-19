/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.index;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * index写入多条docs
 *
 * @since 2025-03-31
 */
public class IndexDocs {
    private static final Logger LOG = LogManager.getLogger(IndexDocs.class);

    public static void indexDocs(RestHighLevelClient highLevelClient, String index) {
        try {
            XContentBuilder builder1 = XContentFactory.jsonBuilder();
            builder1.startObject();
            {
                builder1.field("user", "zhang");
                builder1.field("title", "how to use python");
                builder1.field("content", "java");
                builder1.field("age", "16");
            }
            builder1.endObject();
            IndexRequest indexRequest1 = new IndexRequest(index).id("1").source(builder1);
            IndexResponse indexResponse1 = highLevelClient.index(indexRequest1, RequestOptions.DEFAULT);
            LOG.info("IndexByXContentBuilder response is {}.", indexResponse1.toString());

            XContentBuilder builder2 = XContentFactory.jsonBuilder();
            builder2.startObject();
            {
                builder2.field("user", "li");
                builder2.field("title", "elasticsearch");
                builder2.field("content", "python contents");
                builder2.field("age", "37");
            }
            builder2.endObject();
            IndexRequest indexRequest2 = new IndexRequest(index).id("2").source(builder2);
            IndexResponse indexResponse2 = highLevelClient.index(indexRequest2, RequestOptions.DEFAULT);
            LOG.info("IndexByXContentBuilder response is {}.", indexResponse2.toString());
        } catch (IOException e) {
            LOG.error("IndexByXContentBuilder is failed, exception occurred.", e);
        }
    }

    private static void getIndex(RestHighLevelClient highLevelClient, String index) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(sourceBuilder);

        try {
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("Get index response is {}.", response.toString());
        } catch (IOException e) {
            LOG.error("Get index is failed, exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do index docs.");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            String indexName = "example-huawei";
            indexDocs(highLevelClient, indexName);
            IndexSorting.refresh(highLevelClient, indexName);
            getIndex(highLevelClient, indexName);
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
