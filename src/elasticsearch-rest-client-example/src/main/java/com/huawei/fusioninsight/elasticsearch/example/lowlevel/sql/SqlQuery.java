/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.sql;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.hwclient.HwRestClient;

import com.huawei.fusioninsight.elasticsearch.example.lowlevel.delete.DeleteIndex;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

/**
 * 使用sql操作索引
 *
 * @since 2020-12-24
 */
public class SqlQuery {
    private static final Logger LOG = LogManager.getLogger(SqlQuery.class);

    /**
     * 排序
     */
    private static final String ORDER_BY_SQL = "SELECT account_number FROM example-sql " +
            "ORDER BY account_number DESC";

    /**
     * 聚合、模糊匹配
     */
    private static final String GROUP_BY_HAVING_SQL = "SELECT age, MAX(balance) FROM example-sql " +
            "GROUP BY age HAVING MIN(balance) > 10000";

    /**
     * 拆分文本
     */
    private static final String QUERY_SQL = "SELECT account_number, address FROM example-sql " +
            "WHERE QUERY('address:Lane OR address:Street')";

    private static void bulk(RestClient restClientTest) {
        StringEntity entity;
        String str = "{\"index\":{\"_id\":\"1\"}}\n" +
                "{\"account_number\":1," +
                "\"balance\":39225," +
                "\"firstname\":\"Amber\"," +
                "\"lastname\":\"Duke\"," +
                "\"age\":32," +
                "\"gender\":\"M\"," +
                "\"address\":\"880 Holmes Lane\"," +
                "\"employer\":\"Pyrami\"," +
                "\"email\":\"amberduke@test.com\"," +
                "\"city\":\"Brogan\"," +
                "\"state\":\"IL\"" +
                "}\n " +
                "{\"index\":{\"_id\":\"6\"}}\n " +
                "{\"account_number\":6," +
                "\"balance\":5686," +
                "\"firstname\":\"Hattie\"," +
                "\"lastname\":\"Bond\"," +
                "\"age\":36," +
                "\"gender\":\"M\"," +
                "\"address\":\"671 Bristol Street\"," +
                "\"employer\":\"Netagy\"" +
                ",\"email\":\"hattiebond@test.com\"," +
                "\"city\":\"Dante\"," +
                "\"state\":\"TN\"" +
                "}\n";
        entity = new StringEntity(str, ContentType.APPLICATION_JSON);
        entity.setContentEncoding("UTF-8");
        Response response;
        try {
            Request request = new Request("PUT", "example-sql/_bulk");
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClientTest.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Already input documents.");
            } else {
                LOG.error("Bulk failed.");
            }
            LOG.info("Bulk response entity is : {}.", EntityUtils.toString(response.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("Bulk failed, exception occurred.", e);
        }
    }

    private static void openDistroSqlQuery(RestClient restClient, String sql) {
        Response response;
        try {
            Request request = new Request("POST", "/_opendistro/_sql");
            request.addParameter("pretty", "true");
            String jsonString = "{" + "\"query\":\"" + sql + "\"}";
            HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
            request.setEntity(entity);
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("Sql query successful.");
            } else {
                LOG.error("Sql query failed.");
            }
            LOG.info("Sql query response entity is : {}.", EntityUtils.toString(response.getEntity()));
        } catch (IOException | ParseException e) {
            LOG.error("Sql query failed, exception occurred.", e);
        }
    }

    /**
     * 使用SQL查询索引
     *
     * @param restClient low level rest client
     */
    public static void doSqlQuery(RestClient restClient) {
        bulk(restClient);
        openDistroSqlQuery(restClient, ORDER_BY_SQL);
        openDistroSqlQuery(restClient, GROUP_BY_HAVING_SQL);
        openDistroSqlQuery(restClient, QUERY_SQL);
        DeleteIndex.deleteIndex(restClient, "example-sql");
    }

    public static void main(String[] args) {
        LOG.info("Start to do sql query request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            doSqlQuery(restClient);
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                    LOG.info("Close the client successful.");
                } catch (IOException e1) {
                    LOG.error("Close the client failed.", e1);
                }
            }
        }
    }
}
