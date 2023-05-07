/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.mrs.examples.rest.core;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.protocol.HttpContext;

import java.util.List;
import java.util.Optional;


/**
 * The HttpUtils
 *
 * @since 2020-01-07
 */
public class HttpUtils {
    /**
     * The http request connect timeout.
     */
    public static final Integer DEFAULT_CONNECT_TIMEOUT = 60000;
    /**
     * The socket connect timeout.
     */
    public static final Integer DEFAULT_SOCKET_TIMEOUT = 60000;

    /**
     * Http Get.
     *
     * @param httpClient The http client.
     * @param url Target server url.
     * @param paramList paramL list
     * @param requestConfig requestConfig
     * @param context http context
     * @return HttpResponse.
     * @throws Exception If failed to excute http get action.
     */
    public static HttpResponse httpGet(
            HttpClient httpClient,
            String url,
            List<NameValuePair> paramList,
            RequestConfig requestConfig,
            HttpContext context) throws Exception {
        URIBuilder uriBuilder = new URIBuilder(url);
        if (paramList != null && !paramList.isEmpty()) {
            uriBuilder.setParameters(paramList);
        }

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        setHttpConfig(httpGet, requestConfig);

        if (context == null) {
            return httpClient.execute(httpGet);
        } else {
            return httpClient.execute(httpGet, context);
        }
    }


    /**
     * Http Post.
     *
     * @param httpClient The http client.
     * @param url Target server url.
     * @param entity HttpEntity.
     * @param requestConfig requestConfig
     * @param context http context
     * @param headers http request heads
     * @return HttpResponse.
     * @throws Exception If failed to excute http post action.
     */
    public static HttpResponse httpPost(
            HttpClient httpClient,
            String url,
            HttpEntity entity,
            RequestConfig requestConfig,
            HttpContext context,
            Header[] headers) throws Exception {
        HttpPost httpPost = new HttpPost(url);
        setHttpConfig(httpPost, requestConfig);

        if (entity != null) {
            httpPost.setEntity(entity);
        }

        if (headers != null) {
            for (Header header : headers) {
                httpPost.addHeader(header);
            }
        }

        if (context == null) {
            return httpClient.execute(httpPost);
        } else {
            return httpClient.execute(httpPost, context);
        }
    }

    /**
     * Http Put.
     *
     * @param httpClient The http client.
     * @param url Target server url.
     * @param entity HttpEntity.
     * @param requestConfig Http requestConfig
     * @param context http context
     * @param headers http head
     * @return HttpResponse.
     * @throws Exception If failed to excute http post action.
     */
    public static HttpResponse httpPut(
            HttpClient httpClient,
            String url,
            HttpEntity entity,
            RequestConfig requestConfig,
            HttpContext context,
            Header[] headers) throws Exception {
        HttpPut httpPut = new HttpPut(url);
        setHttpConfig(httpPut, requestConfig);

        if (entity != null) {
            httpPut.setEntity(entity);
        }

        if (headers != null) {
            for (Header header : headers) {
                httpPut.addHeader(header);
            }
        }

        if (context == null) {
            return httpClient.execute(httpPut);
        } else {
            return httpClient.execute(httpPut, context);
        }
    }

    /**
     * Http Delete.
     *
     * @param httpClient The http client.
     * @param url Target server url.
     * @param requestConfig requestConfig
     * @param context context
     * @param headers http head
     * @return HttpResponse.
     * @throws Exception If failed to excute http delete action.
     */
    public static HttpResponse httpDelete(
            HttpClient httpClient,
            String url,
            RequestConfig requestConfig,
            HttpContext context,
            Header[] headers) throws Exception {
        HttpDelete httpDelete = new HttpDelete(url);
        setHttpConfig(httpDelete, requestConfig);

        if (headers != null) {
            for (Header header : headers) {
                httpDelete.addHeader(header);
            }
        }

        if (context == null) {
            return httpClient.execute(httpDelete);
        } else {
            return httpClient.execute(httpDelete, context);
        }
    }

    private static void setHttpConfig(HttpRequestBase httpRequestBase, RequestConfig requestConfig) {
        RequestConfig noNullRequestConfig = Optional.ofNullable(requestConfig)
                .orElse(RequestConfig.custom()
                        .setSocketTimeout(DEFAULT_SOCKET_TIMEOUT)
                        .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
                        .build());
        httpRequestBase.setConfig(noNullRequestConfig);
    }
}
