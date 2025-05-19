/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.highlevel.allrequests;

import com.huawei.fusioninsight.elasticsearch.example.highlevel.cluster.QueryClusterInfo;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.delete.DeleteIndex;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByJson;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByMap;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexByXContentBuilder;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.search.Search;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.search.SearchScroll;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.update.Update;
import com.huawei.fusioninsight.elasticsearch.example.util.HwRestClientUtils;
import com.huawei.fusioninsight.elasticsearch.example.highlevel.index.IndexSorting;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * highclient样例代码入口
 *
 * @since 2020-09-30
 */
public class HighLevelRestClientAllRequests {
    private static final Logger LOG = LoggerFactory.getLogger(HighLevelRestClientAllRequests.class);

    /**
     * Bulk request can be used to to execute multiple index,update or delete
     * operations using a single request.
     */
    private static void bulk(RestHighLevelClient highLevelClient, String index, String id) {
        try {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("user", "Linda");
            jsonMap.put("age", ThreadLocalRandom.current().nextInt(18, 100));
            jsonMap.put("postDate", "2020-01-01");
            jsonMap.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
            jsonMap.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));

            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest(index).id(id).source(jsonMap));
            request.add(new UpdateRequest(index, id).doc(XContentType.JSON, "field", "test information"));
            request.add(new DeleteRequest(index, id));
            BulkResponse bulkResponse = highLevelClient.bulk(request, RequestOptions.DEFAULT);

            if (RestStatus.OK.equals(bulkResponse.status())) {
                LOG.info("Bulk is successful");
            } else {
                LOG.info("Bulk is failed");
            }
        } catch (IOException e) {
            LOG.error("Bulk is failed,exception occurred.", e);
        }
    }

    /**
     * Get index information
     */
    private static void getIndex(RestHighLevelClient highLevelClient, String index, String id) {
        try {
            GetRequest getRequest = new GetRequest(index).id(id);
            String[] includes = new String[] {"message", "test*"};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includes, excludes);
            getRequest.fetchSourceContext(fetchSourceContext);
            getRequest.storedFields("message");
            GetResponse getResponse = highLevelClient.get(getRequest, RequestOptions.DEFAULT);

            LOG.info("GetIndex response is {}", getResponse.toString());
        } catch (IOException e) {
            LOG.error("GetIndex is failed,exception occurred.", e);
        }
    }

    public static void main(String[] args) {
        LOG.info("Start to do high level rest client request!");
        RestHighLevelClient highLevelClient = null;
        HwRestClient hwRestClient = HwRestClientUtils.getHwRestClient(args);
        boolean executeSuccess = true;
        String indexName = "example-huawei";
        try {
            highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder());
            QueryClusterInfo.queryClusterInfo(highLevelClient);
            IndexByJson.indexByJson(highLevelClient, indexName, "1");
            IndexByMap.indexByMap(highLevelClient, indexName, "2");
            IndexByXContentBuilder.indexByXContentBuilder(highLevelClient, indexName, "3");
            Update.update(highLevelClient, indexName, "1");
            bulk(highLevelClient, indexName, "1");
            getIndex(highLevelClient, indexName, "1");
            Search.search(highLevelClient, indexName);
            IndexSorting.doIndexSorting(highLevelClient);
            String scrollId = SearchScroll.searchScroll(highLevelClient, indexName);
            SearchScroll.clearScroll(highLevelClient, scrollId);
            DeleteIndex.deleteIndex(highLevelClient, indexName);
        } finally {
            try {
                if (highLevelClient != null) {
                    highLevelClient.close();
                }
            } catch (IOException e) {
                LOG.error("Failed to close RestHighLevelClient.", e);
                executeSuccess = false;
            }
        }

        if (executeSuccess) {
            LOG.info("Successful execution of elasticsearch rest client example.");
        } else {
            LOG.error("Elasticsearch rest client example execution failed.");
        }
    }
}
