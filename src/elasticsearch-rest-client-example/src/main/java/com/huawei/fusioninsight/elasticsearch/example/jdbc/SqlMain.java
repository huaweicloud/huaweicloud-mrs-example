/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.jdbc;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.delete.DeleteIndex;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * JDBC sql样例代码
 *
 * @since 2020-11-05
 */
public class SqlMain {
    private static final Logger LOG = LogManager.getLogger(SqlMain.class);
    private static void insertData(RestHighLevelClient highLevelClient, String indexName, String username) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            for (int i = 1; i <= 10; i++) {
                BulkRequest request = new BulkRequest();
                for (int j = 1; j <= 100; j++) {
                    jsonMap.clear();
                    jsonMap.put("user", username);
                    jsonMap.put("age", ThreadLocalRandom.current().nextInt(1, 50));
                    jsonMap.put("postDate", "2020-11-3");
                    jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(20, 200));
                    jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(10, 200));
                    if (i % 2 == 0) {
                        jsonMap.put("address", "888 Holmes Lane");
                    } else {
                        jsonMap.put("address", "666 Madison Street");
                    }
                    request.add(new IndexRequest(indexName).source(jsonMap));
                }
                BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);
                if (RestStatus.OK.equals((bulkResponse.status()))) {
                    LOG.info("Bulk is successful.");
                } else {
                    LOG.error("Bulk is failed.");
                }
            }
            RefreshRequest refRequest = new RefreshRequest(indexName);
            highLevelClient.indices().refresh(refRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

    private static boolean createIndexForHighLevel(RestHighLevelClient highLevelClient, String index) {
        try {
            CreateIndexRequest indexRequest = new CreateIndexRequest(index);
            indexRequest
                    .settings(Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1));
            CreateIndexResponse response = highLevelClient.indices().create(indexRequest, RequestOptions.DEFAULT);
            if (response.isAcknowledged() || response.isShardsAcknowledged()) {
                LOG.info("Create index successful by high level client.");
                return true;
            }
        } catch (IOException e) {
            LOG.error("Create index failed.", e);
        }
        return false;
    }

    public static void main(String[] args) {
        try (RestHighLevelClient highLevelClient = new RestHighLevelClient(HwRestClientUtils.getHwRestClient(args).getRestClientBuilder());
             Connection con = new SqlConnect().sqlConnect()){

            boolean isSucceed = true;
            if (!HwRestClientUtils.isExistIndexForHighLevel(highLevelClient, "example-sql1")) {
                isSucceed = createIndexForHighLevel(highLevelClient, "example-sql1");
            }
            if (isSucceed && !HwRestClientUtils.isExistIndexForHighLevel(highLevelClient, "example-sql2")) {
                isSucceed = isSucceed && createIndexForHighLevel(highLevelClient, "example-sql2");
            }

            if (isSucceed) {
                insertData(highLevelClient, "example-sql1", "Tom");
                insertData(highLevelClient, "example-sql2", "Linda");
                SqlSearch.allSearch(con);
                SqlDelete.deleteData(con);
                DeleteIndex.deleteIndex(highLevelClient, "example-sql1");
                DeleteIndex.deleteIndex(highLevelClient, "example-sql2");
            }
        } catch (SQLException sqlException) {
            LOG.error("Filed to close connection.", sqlException);
        } catch (IOException ioException) {
            LOG.error("Failed to close RestHighLevelClient.", ioException);
        } catch (Exception exception) {
            LOG.error("Unknown error.", exception);
        }
    }
}
