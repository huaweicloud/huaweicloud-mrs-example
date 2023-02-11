/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.document;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import com.huawei.fusioninsight.elasticsearch.example.model.Article;
import com.huawei.fusioninsight.elasticsearch.example.util.CommonUtil;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * single doc example
 *
 * @since 2020-09-15
 */
public class SingleDocumentSample {
    private static final Logger LOG = LogManager.getLogger(SingleDocumentSample.class);

    /**
     * 以map方式插入文档
     *
     * @param client 客户端
     */
    public static void createMapDocument(PreBuiltHWTransportClient client) {
        LOG.info("createMapDocument:");
        Map<String, Object> json = new HashMap<>();
        json.put("name", "Elasticsearch Reference");
        json.put("author", "Alex Yang");
        json.put("pubinfo", "Beijing,China.");
        json.put("pubtime", "2016-07-16");
        json.put("desc", "Elasticsearch is a highly scalable open-source full-text search and analytics engine.");
        IndexResponse response;
        try {
            response = client.prepare().prepareIndex("example-book", "book").setSource(json).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        CommonUtil.printIndexInfo(response);
    }

    /**
     * 以Java Bean对象的方式插入文档
     *
     * @param client 客户端
     * @throws JsonProcessingException json处理异常
     */
    public static void createBeanDocument(PreBuiltHWTransportClient client) throws JsonProcessingException {
        LOG.info("createBeanDocument:");
        ObjectMapper mapper = new ObjectMapper();
        AtomicInteger ids = new AtomicInteger(0);
        Article article = new Article(ids.getAndIncrement(), "Elasticsearch Reference",
            "Elasticsearch is a highly scalable open-source full-text search and analytics engine.",
            "https://www.elastic.co/guide/en/elasticsearch/reference/current/getting-started.html",
            Calendar.getInstance().getTime(), "https://www.gitbook.com/@imalexyang/dashboard", "Alex Yang");
        String json = mapper.writeValueAsString(article);
        LOG.info(json);
        createDocument(client, "example-article", "article", json);
    }

    /**
     * 创建文档
     *
     * @param client 客户端
     * @param index 索引名
     * @param type 文档类型
     * @param sourcecontent 文档内容
     */
    private static void createDocument(PreBuiltHWTransportClient client, String index, String type,
        String sourcecontent) {
        LOG.info("createDocument:");
        IndexResponse response;
        try {
            response = client.prepare().prepareIndex(index, type).setSource(sourcecontent, XContentType.JSON).get();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        CommonUtil.printIndexInfo(response);
        LOG.info(index);
    }

    /**
     * 读取文档
     *
     * @param client 客户端
     * @param index 索引名
     * @param type 文档类型
     * @param id 文档id
     */
    public static void getDocument(PreBuiltHWTransportClient client, String index, String type, String id) {
        LOG.info("getDocument:");
        GetResponse response;
        try {
            response = client.prepare().prepareGet(index, type, id).execute().actionGet();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        boolean exists = response.isExists();
        LOG.info("Index found(true of false):{}.", exists);
        LOG.info("response:{}.", response.getSource());
        String responseIndex = response.getIndex();
        String responseType = response.getType();
        String responseId = response.getId();
        long responseVersion = response.getVersion();
        LOG.info("{},{},{},{}.", responseIndex, responseType, responseId, responseVersion);
    }

    /**
     * 删除文档
     *
     * @param client 客户端
     * @param index 索引名
     * @param type 文档类型
     * @param id 文档id
     */
    public static void deleteDocument(PreBuiltHWTransportClient client, String index, String type, String id) {
        try {
            DeleteResponse response = client.prepare().prepareDelete(index, type, id).get();
            DocWriteResponse.Result isFound = response.getResult();
            LOG.info("Index found(true or false):{}.", isFound);
            String responseIndex = response.getIndex();
            String responseType = response.getType();
            String responseId = response.getId();
            // Version (if it's the first time you index this document, you will get: 1)
            long responseVersion = response.getVersion();
            LOG.info("{},{},{},{}.", responseIndex, responseType, responseId, responseVersion);
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
        }
    }

    /**
     * 创建文档id
     *
     * @param client 客户端
     * @param index 索引名称
     * @param type 文档类型
     * @param id 文档id
     */
    public static void createDocumentID(PreBuiltHWTransportClient client, String index, String type, String id) {
        LOG.info("createDocumentID:");
        String sourcecontent =
            "{\"name\":\"Elasticsearch Reference\",\"author\":\"Alex Yang \",\"pubinfo\":\"Beijing,China. \","
                + "\"pubtime\":\"2016-07-16\",\"desc\":\"Elasticsearch is a highly scalable open-source full-text"
                + " search and analytics engine.\"}";
        IndexResponse response;
        try {
            response = client.prepare().prepareIndex(index, type, id).setSource(sourcecontent, XContentType.JSON).get();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        CommonUtil.printIndexInfo(response);
    }

    /**
     * 以String方式插入文档
     *
     * @param client 客户端
     */
    public static void createJsonStringDocument(PreBuiltHWTransportClient client) {
        String json = "{\"name\":\"Elasticsearch Reference\",\"author\":\"Alex Yang \",\"pubinfo\":\"Beijing,China. \","
            + "\"pubtime\":\"2016-07-16\",\"desc\":\"Elasticsearch is a highly scalable open-source full-text"
            + " search and analytics engine.\"}";
        createDocument(client, "example-book", "book", json);
    }

    /**
     * 更新文档内容
     *
     * @param client 客户端
     * @param index 索引名称
     * @param type 文档类型
     * @param id 文档id
     * @param keyValue 包含要更新的字段和值
     * @throws IOException IO异常
     * @throws ExecutionException 执行异常
     * @throws InterruptedException 中断异常
     */
    public static void updateDocument(PreBuiltHWTransportClient client, String index, String type, String id,
        Map<String, String> keyValue) throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index(index);
        updateRequest.type(type);
        updateRequest.id(id);
        String field = keyValue.keySet().iterator().next();
        String value = keyValue.get(field);
        updateRequest.doc(jsonBuilder().startObject().field(field, value).endObject());
        try {
            client.prepare().update(updateRequest).get();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
        }
    }
}
