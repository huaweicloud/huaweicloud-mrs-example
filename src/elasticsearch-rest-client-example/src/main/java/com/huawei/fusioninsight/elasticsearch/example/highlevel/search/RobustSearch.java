/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.search;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * Robust search example: three-layer exception handling + automatic client rebuild
 * <ul>
 *   <li>Layer 1: ES query errors -> silent fallback to empty result</li>
 *   <li>Layer 2: IllegalStateException -> rebuild client then throw</li>
 *   <li>Layer 3: Other exceptions -> check cause chain, rebuild if needed, wrap and throw</li>
 * </ul>
 */
public class RobustSearch {
    private static final Logger LOG = LogManager.getLogger(RobustSearch.class);

    private volatile RestHighLevelClient client;
    private final HwRestClient hwRestClient;

    public RobustSearch(RestHighLevelClient client, HwRestClient hwRestClient) {
        this.client = client;
        this.hwRestClient = hwRestClient;
    }

    /**
     * Search with three-layer exception handling
     */
    public SearchHits search(SearchRequest searchRequest) {
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            return response.getHits();
        } catch (ElasticsearchStatusException | IndexNotFoundException | SearchPhaseExecutionException e) {
            // Layer 1: ES query errors, silent degradation
            LOG.warn("ES query error on index {}: {}",
                    String.join(",", searchRequest.indices()), e.getMessage());
            return SearchHits.empty();
        } catch (IllegalStateException e) {
            // Layer 2: Client state error, attempt rebuild then throw
            rebuildClientIfNeeded(e);
            throw new RuntimeException("ES client invalid state: " + e.getMessage(), e);
        } catch (Exception e) {
            // Layer 3: Other exceptions, check cause chain and rebuild if needed
            if (isCausedBy(e, IllegalStateException.class)) {
                rebuildClientIfNeeded(e);
            }
            throw new RuntimeException("Search failed: " + e.getMessage(), e);
        }
    }

    /**
     * Rebuild client when reactor stopped or connection closed
     */
    private synchronized void rebuildClientIfNeeded(Exception e) {
        String msg = e.getMessage();
        if (msg == null && e.getCause() != null) {
            msg = e.getCause().getMessage();
        }
        if (msg != null && (msg.contains("reactor status: STOPPED") || msg.contains("connection closed"))) {
            LOG.warn("Connection loss detected: {}. Rebuilding client...", msg);
            try {
                if (client != null) {
                    client.close();
                }
                client = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
                LOG.warn("Client rebuilt successfully.");
            } catch (IOException ex) {
                LOG.error("Failed to rebuild client", ex);
            }
        }
    }

    /**
     * Walk the cause chain to check if it contains the specified exception type
     */
    private static boolean isCausedBy(Throwable e, Class<? extends Throwable> type) {
        Throwable current = e;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Entry point for integration into HighLevelRestClientAllRequests
     */
    public static void robustSearch(RestHighLevelClient client, HwRestClient hwRestClient, String indexName) {
        RobustSearch example = new RobustSearch(client, hwRestClient);

        // Prepare test data
        try {
            client.indices().create(new CreateIndexRequest(indexName), RequestOptions.DEFAULT);
            BulkRequest bulk = new BulkRequest();
            for (int i = 1; i <= 3; i++) {
                bulk.add(new IndexRequest(indexName).id(String.valueOf(i))
                        .source("{\"name\":\"user" + i + "\"}", XContentType.JSON));
            }
            client.bulk(bulk, RequestOptions.DEFAULT);
            client.indices().refresh(
                    new org.elasticsearch.action.admin.indices.refresh.RefreshRequest(indexName),
                    RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Failed to prepare test data", e);
        }

        SearchSourceBuilder source = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery()).size(5);

        // Normal query
        SearchHits hits = example.search(new SearchRequest(indexName).source(source));
        LOG.info("Normal query: {} hits", hits.getTotalHits().value);

        // Layer 1 demo: query non-existent index, silent fallback to empty result
        SearchHits empty = example.search(new SearchRequest("non-existent-index").source(source));
        LOG.info("Non-existent index: {} hits (degraded)", empty.getTotalHits().value);

        // Clean up test index
        try {
            client.indices().delete(
                    new org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest(indexName),
                    RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Failed to clean up test index", e);
        }
    }

    public static void main(String[] args) {
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestHighLevelClient client = null;
        try {
            client = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            robustSearch(client, hwRestClient, "example-huawei");
        } finally {
            try {
                if (client != null) {
                    client.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close client", e);
            }
        }
    }
}
