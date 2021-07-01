/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example.document;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import com.huawei.fusioninsight.elasticsearch.example.util.CommonUtil;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;

import java.io.IOException;

/**
 * multi doc example
 *
 * @since 2020-09-15
 */
public class MultiDocumentSample {
    private static final Logger LOG = LogManager.getLogger(MultiDocumentSample.class);

    /**
     * 写入多个文档
     *
     * @param client 客户端
     * @throws IOException jsonBuilder IO异常
     */
    public static void bulkDocuments(PreBuiltHWTransportClient client) throws IOException {
        LOG.info("bulkDocuments:");
        try {
            BulkRequestBuilder bulkRequest = client.prepare().prepareBulk();
            bulkRequest.add(client.prepare()
                .prepareIndex("example-book", "book", "3")
                .setSource(jsonBuilder().startObject()
                    .field("name", "Elasticsearch Reference")
                    .field("author", "Alex Yang")
                    .field("pubinfo", "Beijing,China.")
                    .field("pubtime", "2016-07-16")
                    .field("desc",
                        "Elasticsearch is a highly scalable open-source full-text search" + " and analytics engine.")
                    .endObject()));

            bulkRequest.add(client.prepare()
                .prepareIndex("example-book", "book", "4")
                .setSource(jsonBuilder().startObject()
                    .field("name", "Lucene in Action")
                    .field("author", "Erik Hatcher")
                    .field("pubinfo", "ISBN 9781933988177 532 pages printed in black & white")
                    .field("pubtime", "2004-01-01")
                    .field("desc", "Adding search to your application can be easy. With many reusable"
                        + " examples and good advice on best practices, Lucene in" + " Action shows you how.")
                    .endObject()));
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                LOG.info("Batch indexing fail!");
            } else {
                LOG.info("Batch indexing success!");
            }
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
        }
    }

    /**
     * 获取多个文档
     *
     * @param client 客户端
     */
    public static void mutiGetDocuments(PreBuiltHWTransportClient client) {
        MultiGetResponse multiGetItemResponses;
        try {
            multiGetItemResponses = client.prepare()
                .prepareMultiGet()
                .add("example-twitter", "tweet", "1")
                .add("twitter", "tweet", "2", "3", "4")
                .add("another", "type", "foo")
                .get();
        } catch (ElasticsearchSecurityException e) {
            CommonUtil.handleException(e);
            return;
        }
        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                String json = response.getSourceAsString();
                LOG.info(json);
            }
        }
    }
}
