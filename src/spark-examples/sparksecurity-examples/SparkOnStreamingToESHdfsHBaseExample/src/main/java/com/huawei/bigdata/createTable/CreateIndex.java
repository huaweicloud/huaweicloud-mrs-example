package com.huawei.bigdata.createTable;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateIndex {
    private static final Logger LOG = LoggerFactory.getLogger(CreateIndex.class);

    public static void createIndex(RestHighLevelClient highLevelClient, String[] tableName) throws IOException {
        String estable1_map =
                "{\"properties\":{\"Date\":{\"type\":\"keyword\"},\"Gateway_Name\":{\"type\":\"keyword\"}," +
                        "\"Enterprise_Code\":{\"type\":\"keyword\"},\"Business_Code\":{\"type\":\"keyword\"}}}";
        String estable2_map =
                "{\"properties\":{\"Message_ID\":{\"type\":\"keyword\"},\"Calling_Number\":{\"type\":\"keyword\"}," +
                        "\"Called_Number\":{\"type\":\"keyword\"},\"Submission_Time\":{\"type\":\"keyword\"}," +
                        "\"Final_End_Time\":{\"type\":\"keyword\"}}}";
        List<String> estable_map_list = new ArrayList<>();
        estable_map_list.add(estable1_map);
        estable_map_list.add(estable2_map);

        for (int i = 0; i < tableName.length; i++) {
            String indexName = tableName[i];
            try {
                if (isExistIndexForHighLevel(highLevelClient, indexName)) {
                    System.out.println("Check index successful,index is exist : " + indexName);
                } else {
                    LOG.info("index is not exist : " + indexName);
                    String map_info = estable_map_list.get(i);

                    CreateIndexRequest request = new CreateIndexRequest(indexName);
                    request.settings(Settings.builder()
                            .put("index.number_of_shards", 3)
                            .put("index.number_of_replicas", 2)
                    );
                    request.mapping(
                            "_doc", map_info,
                            XContentType.JSON);

                    CreateIndexResponse createIndexResponse = highLevelClient.indices().create(request, RequestOptions.DEFAULT);
                    if (createIndexResponse.isAcknowledged()) {
                        LOG.info("Create index " + indexName + " is successful.");
                    } else {
                        LOG.info("Create index " + indexName + " is failed.");
                    }

                }
            } catch (Exception e) {
                LOG.error("Check index failed, exception occurred.", e);
            }
        }
    }

    private static boolean isExistIndexForHighLevel(RestHighLevelClient highLevelClient, String indexName) {
        GetIndexRequest isExistsRequest = new GetIndexRequest(indexName);
        try {
            return highLevelClient.indices().exists(isExistsRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            LOG.error("Judge index exist {} failed", indexName, e);
        }
        return false;
    }
}
