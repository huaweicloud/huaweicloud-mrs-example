/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.bulk;

import com.huawei.fusioninsight.elasticsearch.example.lowlevel.exist.IndexIsExist;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.index.CreateIndex;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import com.google.gson.Gson;

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
import java.util.concurrent.ThreadLocalRandom;

/**
 * 批量写入数据
 *
 * @since 2020-09-30
 */
public class Bulk {
    private static final Logger LOG = LogManager.getLogger(Bulk.class);

    /**
     * Send a bulk request
     */
    public static void bulk(RestClient restClientTest, String index) {
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
            StringBuilder builder = new StringBuilder();
            for (int j = 1; j <= oneCommit; j++) {
                esMap.clear();
                esMap.put("name", "Linda");
                esMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
                esMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
                esMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
                esMap.put("cur_time", System.currentTimeMillis());

                String strJson = gson.toJson(esMap);
                builder.append(str).append(System.lineSeparator());
                builder.append(strJson).append(System.lineSeparator());
            }
            entity = new StringEntity(builder.toString(), ContentType.APPLICATION_JSON);
            entity.setContentEncoding("UTF-8");
            Response response;
            try {
                Request request = new Request("PUT", "/_bulk");
                request.addParameter("pretty", "true");
                request.setEntity(entity);
                response = restClientTest.performRequest(request);
                if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                    LOG.info("Already input documents : {}." , oneCommit * i);
                } else {
                    LOG.error("Bulk failed.");
                }
                LOG.info("Bulk response entity is : {}." , EntityUtils.toString(response.getEntity()));
            } catch (IOException | ParseException e) {
                LOG.error("Bulk failed, exception occurred.", e);
            }
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do bulk request.");
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        RestClient restClient = hwRestClient.getRestClient();
        try {
            if (!IndexIsExist.indexIsExist(restClient, "example-huawei1")) {
                CreateIndex.createIndexWithShardNum(restClient, "example-huawei1");
            }
            bulk(restClient, "example-huawei1");
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
