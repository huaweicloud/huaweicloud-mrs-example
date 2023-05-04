/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.springboot.restclient.service;

import com.huawei.fusioninsight.elasticsearch.example.springboot.restclient.client.ElasticsearchConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * elasticsearch springboot样例service
 *
 * @since 2022-11-14
 */
@Service
public class ElasticsearchService {
    private static final Logger LOG = LogManager.getLogger(ElasticsearchService.class);

    @Autowired
    private ElasticsearchConfig elasticsearchRestClient;

    public String getElasticsearchInfo() {
        try {
            MainResponse response = elasticsearchRestClient.elasticsearchClient().info(RequestOptions.DEFAULT);
            String clusterName = response.getClusterName();
            String result = "ClusterName:[" + clusterName + "], clusterUuid:[" + response.getClusterUuid() +
                "], nodeName:[" + response.getNodeName() + "], version:[" + response.getVersion().toString() + "].\"";
            return result;
        } catch (IOException e) {
            LOG.error("QueryClusterInfo is failed,exception occurred.", e);
        }

        return "Fail to execute get elasticsearch cluster info.";
    }

    public String getElasticsearchClusterHealth() {
        try {
            ClusterHealthResponse response = elasticsearchRestClient.elasticsearchClient()
                .cluster()
                .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
            return response.toString();
        } catch (IOException e) {
            LOG.error("Fail to execute get elasticsearch cluster health.", e);
        }

        return "Fail to execute get elasticsearch cluster health.";
    }

    public String indexByJson(String index) {
        try {
            IndexRequest indexRequest = new IndexRequest(index);
            String jsonString =
                "{\"user\":\"kimchy1\",\"age\":\"100\",\"postDate\":\"2020-01-01\",\"message\":\"trying out Elasticsearch\",\"reason\":\"daily update\",\"innerObject1\":\"Object1\","
                    + "\"innerObject2\":\"Object2\",\"innerObject3\":\"Object3\",\"uid\":\"11\"}";
            indexRequest.source(jsonString, XContentType.JSON);
            IndexResponse indexResponse = elasticsearchRestClient.elasticsearchClient().index(indexRequest, RequestOptions.DEFAULT);
            return "IndexByJson response is " + indexResponse.toString() + ".";
        } catch (IOException e) {
            LOG.error("IndexByJson is failed,exception occurred.", e);
        }

        return "Fail to create index by json.";
    }

    public String indexByMap(String index) {
        try {
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("user", "kimchy2");
            dataMap.put("age", "200");
            dataMap.put("postDate", new Date());
            dataMap.put("message", "trying out Elasticsearch");
            dataMap.put("reason", "daily update");
            dataMap.put("innerObject1", "Object1");
            dataMap.put("innerObject2", "Object2");
            dataMap.put("innerObject3", "Object3");
            dataMap.put("uid", "22");
            IndexRequest indexRequest = new IndexRequest(index).source(dataMap);
            IndexResponse indexResponse = elasticsearchRestClient.elasticsearchClient().index(indexRequest, RequestOptions.DEFAULT);
            return "IndexByMap response is " + indexResponse.toString() + ".";
        } catch (Exception e) {
            LOG.error("IndexByMap is failed, exception occurred.", e);
        }

        return "Fail to create index by map.";
    }

    public String indexByXContentBuilder(String index) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user", "kimchy3");
                builder.field("age", "300");
                builder.field("postDate", "2020-01-01");
                builder.field("message", "trying out Elasticsearch");
                builder.field("reason", "daily update");
                builder.field("innerObject1", "Object1");
                builder.field("innerObject2", "Object2");
                builder.field("innerObject3", "Object3");
                builder.field("uid", "33");
            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(index).source(builder);
            IndexResponse indexResponse = elasticsearchRestClient.elasticsearchClient().index(indexRequest, RequestOptions.DEFAULT);
            return "IndexByXContentBuilder response is " + indexResponse.toString() + ".";
        } catch (IOException e) {
            LOG.error("IndexByXContentBuilder is failed, exception occurred.", e);
        }

        return "Fail to create index by XContentBuilder.";
    }

    public String update(String index, String id) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            { // update information
                builder.field("postDate", new Date());
                builder.field("reason", "update again");
            }
            builder.endObject();
            UpdateRequest request = new UpdateRequest(index, id).doc(builder);
            UpdateResponse updateResponse = elasticsearchRestClient.elasticsearchClient().update(request, RequestOptions.DEFAULT);
            return "Update response is " + updateResponse.toString() + ".";
        } catch (IOException e) {
            LOG.error("Update is failed,exception occurred.", e);
        }

        return "Fail to update index: " + index + ", id: " + id + ".";
    }

    public String bulk(String index) {
        boolean isCreateSuccess = true;
        if (!isExistIndexForHighLevel(elasticsearchRestClient.elasticsearchClient(), index)) {
            isCreateSuccess = createIndexForHighLevel(elasticsearchRestClient.elasticsearchClient(), index);
        }

        if (isCreateSuccess) {
            bulkData(elasticsearchRestClient.elasticsearchClient(), index);
            return "Success to execute bulk.";
        } else {
            LOG.error("Create index {} failed.", index);
        }

        return "Fail to execute bulk.";
    }

    public boolean isExistIndexForHighLevel(RestHighLevelClient highLevelClient, String indexName) {
        GetIndexRequest isExistsRequest = new GetIndexRequest(indexName);
        try {
            return highLevelClient.indices().exists(isExistsRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Judge index exist {} failed", indexName, e);
        }
        return false;
    }

    public boolean createIndexForHighLevel(RestHighLevelClient highLevelClient, String indexName) {
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
            indexRequest.settings(
                Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1));
            CreateIndexResponse response = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
            if (response.isAcknowledged() || response.isShardsAcknowledged()) {
                LOG.info("Create index {} successful by high level client.", indexName);
                return true;
            }
        } catch (IOException e) {
            LOG.error("Create index failed.", e);
        }
        return false;
    }

    public void bulkData(RestHighLevelClient highLevelClient, String index) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            for (int i = 1; i <= 100; i++) {
                BulkRequest request = new BulkRequest();
                for (int j = 1; j <= 1000; j++) {
                    jsonMap.clear();
                    jsonMap.put("user", "Linda");
                    jsonMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                    jsonMap.put("postDate", "2020-01-01");
                    jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                    jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                    request.add(new IndexRequest(index).source(jsonMap));
                }
                BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);

                if (RestStatus.OK.equals((bulkResponse.status()))) {
                    LOG.info("Bulk is successful");
                } else {
                    LOG.info("Bulk is failed");
                }
            }
        } catch (IOException e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

    public String getIndex(String index, String id) {
        try {
            GetRequest getRequest = new GetRequest(index).id(id);
            String[] includes = new String[]{"message", "test*"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            getRequest.fetchSourceContext(fetchSourceContext);
            getRequest.storedFields("message");
            GetResponse getResponse = elasticsearchRestClient.elasticsearchClient().get(getRequest, RequestOptions.DEFAULT);
            return "GetIndex response is " + getResponse.toString() + ".";
        } catch (IOException e) {
            LOG.error("GetIndex is failed,exception occurred.", e);
        }

        return "Fail to get index: " + index + ", id: " + id + ".";
    }

    public String search(String index) {
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.termQuery("user", "kimchy1"));

            searchSourceBuilder.sort(new FieldSortBuilder("_doc").order(SortOrder.ASC));

            // Source filter
            String[] includeFields = new String[]{"message", "user", "innerObject*"};
            String[] excludeFields = new String[]{"postDate"};
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
            SearchResponse searchResponse = elasticsearchRestClient.elasticsearchClient().search(searchRequest, RequestOptions.DEFAULT);
            return "Search response is " + searchResponse.toString() + ".";
        } catch (IOException e) {
            LOG.error("Search is failed, exception occurred.", e);
        }

        return "Fail to search index: " + index + ".";
    }

    public String searchScroll(String index) {
        String scrollId;
        try {
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchQuery("title", "Elasticsearch"));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = elasticsearchRestClient.elasticsearchClient().search(searchRequest, RequestOptions.DEFAULT);
            scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            LOG.info("SearchHits is {}", searchResponse.toString());

            while (searchHits != null && searchHits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = elasticsearchRestClient.elasticsearchClient().scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
                LOG.info("SearchHits is {}", searchResponse.toString());
            }
        } catch (IOException e) {
            LOG.error("SearchScroll is failed, exception occurred.", e);
            scrollId = null;
        }
        return scrollId;
    }

    public String clearScroll(String scrollId) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse;
        try {
            clearScrollResponse = elasticsearchRestClient.elasticsearchClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            if (clearScrollResponse.isSucceeded()) {
                return "ClearScroll is successful.";
            } else {
                return "ClearScroll is failed.";
            }
        } catch (IOException e) {
            LOG.error("ClearScroll is failed, exception occurred.", e);
        }

        return "Fail to clear scroll.";
    }

    public String deleteIndex(String index) {
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteResponse = elasticsearchRestClient.elasticsearchClient().indices().delete(request, RequestOptions.DEFAULT);
            if (deleteResponse.isAcknowledged()) {
                return "Delete index: " + index + " is successful.";
            } else {
                return "Delete index: " + index + " is failed.";
            }
        } catch (IOException e) {
            LOG.error("Delete index: {} is failed, exception occurred.", index, e);
        }

        return "Fail to delete index: " + index + ".";
    }
}
