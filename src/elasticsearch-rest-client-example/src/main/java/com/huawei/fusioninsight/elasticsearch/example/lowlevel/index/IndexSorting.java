/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.index;

import com.huawei.fusioninsight.elasticsearch.example.lowlevel.delete.DeleteIndex;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.exist.IndexIsExist;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.bulk.Bulk;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * 索引预排序样例
 *
 * @since 2023-01-10
 */
public class IndexSorting {
    private static final Logger LOG = LogManager.getLogger(IndexSorting.class);

    private static void createIndexWithSorting(RestClient restClient, String index) {
        Response rsp;
        String jsonString;
        jsonString = "{\"settings\":{\"number_of_shards\":\"3\", \"number_of_replicas\":\"1\","
            + "\"sort.field\": [\"height\",\"weight\"],\"sort.order\": [\"desc\",\"asc\"]},\"mappings\":{"
            + "\"properties\":{\"height\":{\"type\":\"float\"},\"weight\":{\"type\":\"float\"}}}}";
        HttpEntity entity = new StringEntity(jsonString, ContentType.APPLICATION_JSON);
        try {
            Request request = new Request("PUT", "/" + index);
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            rsp = restClient.performRequest(request);
            if (rsp.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                LOG.info("Create index with sorting successfully.");
            } else {
                LOG.error("Create index with sorting failed.");
            }
            LOG.info("Create index with sorting response entity is {}.", EntityUtils.toString(rsp.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("Failed to create index with sorting, exception occurred.", e);
        }
    }

    private static void refresh(RestClient restClient, String index) {
        try {
            Map<String, String> params = Collections.singletonMap("pretty", Boolean.TRUE.toString());
            Response rsp = restClient.performRequest(buildRequest("POST", "/" + index + "/_refresh", params, null));
            if (rsp.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                LOG.info("Refresh sorting index successfully.");
            }
        } catch (IOException e) {
            LOG.error("Failed to refresh sorting index, exception occurred.", e);
        }
    }

    private static Request buildRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity) {
        Request request = new Request(method, endpoint);
        if (params != null) {
            params.forEach(request::addParameter);
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return request;
    }

    private static void queryData(RestClient restClient, String index) {
        Response response;
        String jsonStr;
        jsonStr = "{\"sort\":[{\"height\": \"desc\"," + "\"weight\": \"asc\"}],\"track_total_hits\": false}";
        StringEntity entity = new StringEntity(jsonStr, ContentType.APPLICATION_JSON);
        try {
            Request request = new Request("GET", "/" + index + "/_search");
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClient.performRequest(request);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                LOG.info("Query sorting index successfully.");
            } else {
                LOG.error("Failed to query sorting index.");
            }
            LOG.info("Query sorting index response entity is {}.", EntityUtils.toString(response.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("Failed to query sorting index, exception occurred.", e);
        }
    }

    /**
     * index sorting sample
     *
     * @param restClient low level 客户端
     */
    public static void doIndexSorting(RestClient restClient) {
        String index = "example-sort";
        if (IndexIsExist.indexIsExist(restClient, index)) {
            DeleteIndex.deleteIndex(restClient, index);
        }
        createIndexWithSorting(restClient, index);
        Bulk.bulk(restClient, index);
        refresh(restClient, index);
        queryData(restClient, index);
    }

    private static void closeClient(RestClient restClient) {
        if (restClient != null) {
            try {
                restClient.close();
                LOG.info("Close the client successfully.");
            } catch (IOException e1) {
                LOG.error("Failed to close the client.", e1);
            }
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do index sorting.");
        RestClient restClient = null;
        try {
            HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
            restClient = hwRestClient.getRestClient();
            doIndexSorting(restClient);
        } finally {
            closeClient(restClient);
        }
    }
}
