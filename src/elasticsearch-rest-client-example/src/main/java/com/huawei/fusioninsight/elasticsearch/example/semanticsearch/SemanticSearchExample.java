/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.semanticsearch;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.allrequests.HighLevelRestClientAllRequests;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;
import com.huawei.us.common.file.UsFileUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

/**
 * 语义向量插件样例
 *
 * @since 2025-02-12
 */
public class SemanticSearchExample {
    private static final Logger LOG = LoggerFactory.getLogger(HighLevelRestClientAllRequests.class);

    private static String confPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

    private static final String INDEX_STANDARD_MAPPING = loadFromResource("mappings.json");

    private static String embeddingUrl;
    private static String embeddingModelName;
    private static String embeddingFaqModelName;
    private static String rerankModelName;
    private static String semanticIndexName;

    /**
     * Configure the embedding model.
     */
    private static void createEmbeddingModel(RestHighLevelClient highLevelClient) {
        try {
            String jsonString =
                "{\"description\": \"盘古搜索大模型-语义向量化\", \n" +
                    "  \"service_config\": { \n" +
                    "    \"semantic_vector\": { \n" +
                    "      \"service_urls\": [\"" + embeddingUrl + "/pangu/search/v1/vector\"] \n" +
                    "    } \n" +
                    "  } \n}";
            Request request = new Request(
                "PUT",
                "/_inference/model_service/" + embeddingModelName
            );
            request.setJsonEntity(jsonString);
            Response response = highLevelClient.getLowLevelClient().performRequest(request);
            LOG.info("CreateVectorModel response is {}.", response.toString());
        } catch (IOException e) {
            LOG.error("CreateVectorModel is failed, exception occurred.", e);
        }
    }

    /**
     * Configure the rerank model.
     */
    private static void createRerankModel(RestHighLevelClient highLevelClient) {
        try {
            String jsonString =
                "{\"description\": \"盘古搜索大模型-精排模型\", \n" +
                    "  \"service_config\": { \n" +
                    "     \"reorder\": { \n" +
                    "        \"service_urls\": [\"" + embeddingUrl + "/pangu/search/v1/rerank\"] \n" +
                    "    } \n" +
                    "  } \n}";
            Request request = new Request(
                "PUT",
                "/_inference/model_service/" + rerankModelName
            );
            request.setJsonEntity(jsonString);
            Response response = highLevelClient.getLowLevelClient().performRequest(request);
            LOG.info("CreateRerankModel response is {}.", response.toString());
        } catch (IOException e) {
            LOG.error("CreateRerankModel is failed, exception occurred.", e);
        }
    }

    /**
     * Configure the embedding faq model.
     */
    private static void createEmbeddingFaqModel(RestHighLevelClient highLevelClient) {
        try {
            String jsonString =
                "{ \n" +
                    "  \"description\": \"盘古搜索大模型-query2query\", \n" +
                    "  \"service_config\": { \n" +
                    "    \"semantic_vector\": { \n" +
                    "      \"embedding_type\" : \"query2query\",\n" +
                    "      \"service_urls\": [\"" + embeddingUrl + "/pangu/search/v1/vector\"] \n" +
                    "    } \n" +
                    "  } \n" +
                    "}";
            Request request = new Request(
                "PUT",
                "/_inference/model_service/" + embeddingFaqModelName
            );
            request.setJsonEntity(jsonString);
            Response response = highLevelClient.getLowLevelClient().performRequest(request);
            LOG.info("CreateEmbeddingFaqModel response is {}.", response.toString());
        } catch (IOException e) {
            LOG.error("CreateEmbeddingFaqModel is failed, exception occurred.", e);
        }
    }

    /**
     * Get the inference service information.
     */
    private static void getInferenceService(RestHighLevelClient highLevelClient) {
        try {
            Request request = new Request(
                "GET",
                "/_inference/model_service?pretty"
            );
            Response response = highLevelClient.getLowLevelClient().performRequest(request);
            LOG.info("GetInferenceService response is {}.", EntityUtils.toString(response.getEntity(), ContentType.DEFAULT_TEXT.getCharset()));
        } catch (IOException e) {
            LOG.error("GetInferenceService is failed, exception occurred.", e);
        }
    }

    /**
     * Load semantic index mappings and settings from mappings.json.
     */
    public static String loadFromResource(String resourceFilePath) {
        if (StringUtils.isBlank(resourceFilePath)) {
            return null;
        }
        String content = "";
        try {
            Resource resource = new DefaultResourceLoader().getResource("classpath:" + resourceFilePath);
            content = IOUtils.toString(resource.getInputStream(), StandardCharsets.UTF_8);
        } catch (FileNotFoundException e) {
            LOG.error("Failed to load resource file.");
        } catch (IOException e) {
            LOG.error("Failed to load resource file, exception occurred.");
        }
        return content;
    }


    /**
     * Create the semantic vector index.
     */
    private static void createSemanticIndex(RestHighLevelClient highLevelClient, String indexName) {
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest(indexName);
            String mappingsStr = String.format(INDEX_STANDARD_MAPPING, embeddingModelName, rerankModelName);
            indexRequest.source(mappingsStr, XContentType.JSON);
            CreateIndexResponse indexResponse = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
            LOG.info("CreateSemanticIndex response is {}.", indexResponse.toString());
        } catch (IOException e) {
            LOG.error("CreateSemanticIndex is failed, exception occurred.", e);
        }
    }

    /**
     * Bulk data into semantic vector index.
     */
    private static void bulkData(RestHighLevelClient highLevelClient, String indexName) {
        try {
            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add(new IndexRequest(indexName).id("1")
                .source(XContentType.JSON, "title", "Java documentation",
                    "desc", "Java uses an automatic garbage collector to manage memory in the object lifecycle.",
                    "content", "Java", "author", "张三"));
            BulkResponse bulkResponse = highLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            LOG.info("BulkData response is {}.", bulkResponse.toString());
        } catch (IOException e) {
            LOG.error("BulkData is failed, exception occurred.", e);
        }
    }

    /**
     * Get content of the semantic vector index.
     */
    private static void getIndex(RestHighLevelClient highLevelClient, String indexName) {
        try {
            Request request = new Request(
                "GET",
                indexName + "/_search?pretty"
            );
            Response response = highLevelClient.getLowLevelClient().performRequest(request);
            LOG.info("GetIndex response is {}.", EntityUtils.toString(response.getEntity(), ContentType.DEFAULT_TEXT.getCharset()));
        } catch (IOException e) {
            LOG.error("GetIndex is failed, exception occurred.", e);
        }
    }

    /**
     * Execute semantic query.
     */
    private static void semanticSearch(RestHighLevelClient highLevelClient, String indexName) {
        try {
            SearchRequest searchRequest = new SearchRequest(indexName);
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            // 设置_source排除字段
            sourceBuilder.fetchSource(null, new String[]{"_inference"});

            // 构建查询条件
            BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
            MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery("How to manage memory for Java?",
                "title", "content", "desc");
            boolQuery.must(multiMatchQuery);

            sourceBuilder.query(boolQuery);
            searchRequest.source(sourceBuilder);
            SearchResponse response = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);
            LOG.info("SemanticSearch response is {}", response.toString());
        } catch (IOException e) {
            LOG.error("SemanticSearch is failed, exception occurred.", e);
        }
    }

    /**
     * Refresh semantic vector index.
     */
    private static void refresh(RestHighLevelClient highLevelClient, String index) {
        try {
            RefreshRequest refRequest = new RefreshRequest(index);
            highLevelClient.indices().refresh(refRequest, RequestOptions.DEFAULT);
            LOG.info("Refresh index successfully.");
        } catch (IOException e) {
            LOG.error("Failed to refresh index, exception occurred.", e);
        }
    }

    private static void initProperties() {
        Properties properties = new Properties();
        String proPath = confPath + "esParams.properties";
        try {
            properties.load(Files.newInputStream(UsFileUtils.getFile(proPath).toPath()));
        } catch (IOException e) {
            LOG.error("Failed to load properties file.", e);
        }

        embeddingUrl = properties.getProperty("embeddingUrl");
        embeddingModelName = properties.getProperty("embeddingModelName");
        rerankModelName = properties.getProperty("rerankModelName");
        embeddingFaqModelName = properties.getProperty("embeddingFaqModelName");
        semanticIndexName = properties.getProperty("semanticIndexName");
    }

    public static void main(String[] args) {
        LOG.info("Start to do high level rest client request!");
        initProperties();
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        boolean executeSuccess = true;
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());

            createEmbeddingModel(highLevelClient);
            createRerankModel(highLevelClient);
            createEmbeddingFaqModel(highLevelClient);
            getInferenceService(highLevelClient);
            createSemanticIndex(highLevelClient, semanticIndexName);
            bulkData(highLevelClient, semanticIndexName);
            refresh(highLevelClient, semanticIndexName);
            getIndex(highLevelClient, semanticIndexName);
            semanticSearch(highLevelClient, semanticIndexName);
        } finally {
            try {
                if (highLevelClient != null) {
                    highLevelClient.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close RestHighLevelClient.", e);
                executeSuccess = false;
            }
        }

        if (executeSuccess) {
            LOG.info("Successful execution of elasticsearch pg search example.");
        } else {
            LOG.error("Elasticsearch pg search example execution failed.");
        }
    }
}
