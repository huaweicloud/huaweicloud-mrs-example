/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 利用HttpClient进行post请求的工具类
 * 
 * @since 2020/10/10
 */
public class HttpClientUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientUtil.class);

    /**
     * post请求
     * 
     * @param url url 
     * @param jsonstr requestBody json
     * @param charset 字符集
     * @param isSecurity 非安全集群可以不需要token，直接连接
     * @return 返回结果
     */
    public static String doPost(String url, String jsonstr, String charset, boolean isSecurity)
        throws Exception {
        HttpClient httpClient = null;
        HttpPost httpPost = null;
        String result = null;
        try {
            httpClient = new SSLClient();
            httpPost = new HttpPost(url);
            httpPost.addHeader("Content-Type", "application/json");
            // 关键步骤，在Cookie中加入token
            if (isSecurity) {
                httpPost.addHeader("Cookie", LoginClient.getInstance().getToken());
            } else {
                httpPost.addHeader("NormalModleUser", LoginClient.getInstance().getPrincipal());
            }
            StringEntity se = new StringEntity(jsonstr);
            se.setContentType("application/json");
            se.setContentEncoding(new BasicHeader("Content-Type", "application/json"));
            httpPost.setEntity(se);
            HttpResponse response = httpClient.execute(httpPost);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Http client post action error", ex);
            throw ex;
        }
        return result;
    }

    /**
     * get请求
     *
     * @param url url 
     * @param charset 字符集
     * @return 返回结果
     */
    public static String doGet(String url, String charset) throws Exception {
        HttpClient httpClient = null;
        HttpGet httpGet = null;
        String result = null;
        try {
            httpClient = new SSLClient();
            httpGet = new HttpGet(url);
            httpGet.addHeader("Content-Type", "application/json");
            httpGet.addHeader("Cookie", LoginClient.getInstance().getToken());
            HttpResponse response = httpClient.execute(httpGet);
            if (response != null) {
                HttpEntity resEntity = response.getEntity();
                if (resEntity != null) {
                    result = EntityUtils.toString(resEntity, charset);
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Http client get action error", ex);
            throw ex;
        }
        return result;
    }
}
