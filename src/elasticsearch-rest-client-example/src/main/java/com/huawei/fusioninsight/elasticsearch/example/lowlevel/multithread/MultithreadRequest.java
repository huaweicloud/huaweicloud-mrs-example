/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.multithread;

import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 多线程请求
 *
 * @since 2020-09-30
 */
public class MultithreadRequest {
    private static final Logger LOG = LogManager.getLogger(MultithreadRequest.class);

    private static final String INDEX_SETTING_KEY = "index";

    /**
     * Send a bulk request
     */
    private static void bulk(RestClient restClientTest, String index) {
        // 需要写入的总文档数
        long totalRecordNum = 10000;
        // 一次bulk写入的文档数,推荐一次写入的大小为5M-15M
        long oneCommit = 1000;
        long circleNumber = totalRecordNum / oneCommit;
        StringEntity entity;
        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<>();
        String str = "{ \"index\" : { \"_index\" : \"" + index + "\"} }";

        for (int i = 1; i <= circleNumber; i++) {
            StringBuffer buffer = new StringBuffer();
            for (int j = 1; j <= oneCommit; j++) {
                esMap.clear();
                esMap.put("name", "Linda");
                esMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                esMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                esMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                esMap.put("cur_time", System.currentTimeMillis());
                String esStr = gson.toJson(esMap);
                buffer.append(str).append(System.lineSeparator());
                buffer.append(esStr).append(System.lineSeparator());
            }
            entity = new StringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
            entity.setContentEncoding("UTF-8");
            Response response;
            try {
                Request request = new Request("PUT", "/_bulk");
                request.addParameter("pretty", "true");
                request.setEntity(entity);
                response = restClientTest.performRequest(request);
                if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                    // bulk请求中如果有失败的条目，请在业务代码中进行重试处理
                    boolean responseAllSuccess = parseResponse(EntityUtils.toString(response.getEntity()));
                    if (!responseAllSuccess) {
                        LOG.warn("There are some error in bulk response,please retry in client.");
                    } else {
                        LOG.info("Bulk successful");
                    }
                } else {
                    LOG.warn("Bulk failed.");
                }
            } catch (IOException | ParseException e) {
                LOG.error("Bulk failed, exception occurred.", e);
            }
        }
    }

    /**
     * parse response
     *
     * @param content httpclient response
     * @return 请求是否执行成功
     */
    private static boolean parseResponse(String content) {
        JsonObject object = new JsonParser().parse(content).getAsJsonObject();
        JsonArray array = object.getAsJsonArray("items");
        long successNum = 0L;
        long failedNum = 0L;
        int status;
        for (int i = 0; i < array.size(); i++) {
            JsonObject obj;
            JsonElement arrayElement = array.get(i);
            JsonElement indexSettingKey = null;
            if (arrayElement instanceof JsonObject) {
                indexSettingKey = ((JsonObject) arrayElement).get(INDEX_SETTING_KEY);
            }
            if (indexSettingKey instanceof JsonObject) {
                obj = (JsonObject) indexSettingKey;
            } else {
                LOG.error("Response format is incorrect.");
                return false;
            }
            status = obj.get("status").getAsInt();
            if (HttpStatus.SC_OK != status && HttpStatus.SC_CREATED != status) {
                LOG.debug("Error response is {}", obj.toString());
                failedNum++;
            } else {
                successNum++;
            }
        }
        if (failedNum > 0) {
            LOG.info("Response has error,successNum is {},failedNum is {}.", successNum, failedNum);
            return false;
        } else {
            LOG.info("Response has no error.");
            return true;
        }
    }

    /**
     * 发送请求的线程
     *
     * @since 2020-09-30
     */
    public static class SendRequestThread implements Runnable {
        private RestClient restClientTh;

        private HwRestClient hwRestClient;

        private String[] args;

        public SendRequestThread(String[] args) {
            this.args = args;
        }

        @Override
        public void run() {
            LOG.info("Thread begin.");
            try {
                hwRestClient = HwRestClientUtils.getHwRestClient(args);
                restClientTh = hwRestClient.getRestClient();
                LOG.info("Thread name: {}.", Thread.currentThread().getName());
                bulk(restClientTh, "example-huawei1");
            } finally {
                if (restClientTh != null) {
                    try {
                        restClientTh.close();
                        LOG.info("Close the client successful in thread : {}.", Thread.currentThread().getName());
                    } catch (IOException e) {
                        LOG.error("Close the client failed.", e);
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do multithread request !");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClientThNew;
        restClientThNew = hwRestClient.getRestClient();
        int threadPoolSize = 5;
        int jobNumber = 5;
        ThreadPoolExecutor pool = new ThreadPoolExecutor(threadPoolSize, threadPoolSize, 60L, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(threadPoolSize));
        try {
            for (int i = 0; i < jobNumber; i++) {
                pool.execute(new SendRequestThread(args));
            }
            pool.shutdown();
        } finally {
            if (restClientThNew != null) {
                try {
                    restClientThNew.close();
                    LOG.info("Close the client successful.");
                } catch (IOException e1) {
                    LOG.error("Close the client failed.", e1);
                }
            }
        }
    }
}
