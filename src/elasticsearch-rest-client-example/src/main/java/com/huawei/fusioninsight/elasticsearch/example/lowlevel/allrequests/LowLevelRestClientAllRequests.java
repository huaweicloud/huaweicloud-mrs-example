/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.lowlevel.allrequests;

import com.huawei.fusioninsight.elasticsearch.example.lowlevel.bulk.BulkRoutingSample;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.cluster.QueryClusterInfo;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.delete.DeleteIndex;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.delete.DeleteSomeDocumentsInIndex;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.exist.IndexIsExist;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.flush.Flush;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.index.CreateIndex;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.index.IndexSorting;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.index.PutData;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.search.QueryData;
import com.huawei.fusioninsight.elasticsearch.example.lowlevel.sql.SqlQuery;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;

import com.google.gson.Gson;

import org.apache.http.HttpStatus;
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
 * lowclient 样例代码入口
 *
 * @since 2020-09-30
 */
public class LowLevelRestClientAllRequests {
    private static final Logger LOG = LogManager.getLogger(LowLevelRestClientAllRequests.class);

    /**
     * Send a bulk request
     */
    private static void bulk(RestClient restClientTest, String index, String type) {
        // 需要写入的总文档数
        long totalRecordNum = 10;
        // 一次bulk写入的文档数
        long oneCommit = 5;
        long circleNumber = totalRecordNum / oneCommit;
        StringEntity entity;
        Gson gson = new Gson();
        Map<String, Object> esMap = new HashMap<>();
        String str = "{ \"index\" : { \"_index\" : \"" + index + "\", \"_type\" :  \"" + type + "\"} }";

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
                request.setEntity(entity);
                request.addParameter("pretty", "true");
                response = restClientTest.performRequest(request);
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    LOG.info("Already input documents {}.", oneCommit * i);
                } else {
                    LOG.error("Bulk failed.");
                }
                LOG.info("Bulk response entity is {}.", EntityUtils.toString(response.getEntity()));
            } catch (IOException e) {
                LOG.error("Bulk failed, exception occurred.", e);
            }
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do low level rest client request.");
        RestClient restClient = null;
        boolean executeSuccess = true;
        String indexName = "example-huawei";
        try {
            HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
            restClient = hwRestClient.getRestClient();
            QueryClusterInfo.queryClusterInfo(restClient);
            if (IndexIsExist.indexIsExist(restClient, indexName)) {
                DeleteIndex.deleteIndex(restClient, indexName);
            }
            CreateIndex.createIndexWithShardNum(restClient, indexName);
            PutData.putData(restClient, indexName, "_doc", "1");
            bulk(restClient, indexName, "_doc");
            BulkRoutingSample.bulkWithRouting(restClient, indexName);
            QueryData.queryData(restClient, indexName, "_doc", "1");
            IndexSorting.doIndexSorting(restClient);
            SqlQuery.doSqlQuery(restClient);
            DeleteSomeDocumentsInIndex.deleteSomeDocumentsInIndex(restClient, indexName, "pubinfo", "Beijing");
            Flush.flushOneIndex(restClient, indexName);
            DeleteIndex.deleteIndex(restClient, indexName);
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                    LOG.info("Close the client successful.");
                } catch (IOException e1) {
                    LOG.error("Close the client failed.", e1);
                    executeSuccess = false;
                }
            }
        }

        if (executeSuccess) {
            LOG.info("Successful execution of elasticsearch rest client example.");
        } else {
            LOG.error("Elasticsearch rest client example execution failed.");
        }
    }
}
