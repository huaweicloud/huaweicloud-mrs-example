/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.fusioninsight.elasticsearch.example;

import com.huawei.fusioninsight.elasticsearch.example.cluster.ClusterSample;
import com.huawei.fusioninsight.elasticsearch.example.document.MultiDocumentSample;
import com.huawei.fusioninsight.elasticsearch.example.document.SingleDocumentSample;
import com.huawei.fusioninsight.elasticsearch.example.indices.IndicesSample;
import com.huawei.fusioninsight.elasticsearch.example.search.SearchSample;
import com.huawei.fusioninsight.elasticsearch.transport.client.ClientFactory;
import com.huawei.fusioninsight.elasticsearch.transport.client.PreBuiltHWTransportClient;

/**
 * transport样例代码
 *
 * @since 2020-03-30
 */
public class Sample {
    /**
     * 样例代码入口
     *
     * @param args 参数列表
     * @throws Exception 异常
     */
    public static void main(String[] args) throws Exception {
        String bookIndex = "example-book";
        String articleIndex = "example-article";
        String tweetIndex = "example-tweet";
        ClientFactory.initConfiguration(LoadProperties.loadProperties(args));
        PreBuiltHWTransportClient client = ClientFactory.getClient();
        ClusterSample.clusterHealth(client);
        IndicesSample.createIndexWithSettings(client, articleIndex);
        IndicesSample.createIndexWithMapping(client, tweetIndex);
        SingleDocumentSample.createBeanDocument(client);
        SingleDocumentSample.getDocument(client, articleIndex, "article", "1");
        ClusterSample.clusterHealth(client);
        SingleDocumentSample.createMapDocument(client);
        SingleDocumentSample.createDocumentID(client, bookIndex, "book", "1");
        SingleDocumentSample.createDocumentID(client, bookIndex, "book", "2");
        SingleDocumentSample.createJsonStringDocument(client);
        SingleDocumentSample.createDocumentID(client, bookIndex, "book", "10");
        MultiDocumentSample.bulkDocuments(client);
        SearchSample.scrollSearchDelete(client, articleIndex, "content", "elasticsearch");
        SingleDocumentSample.getDocument(client, bookIndex, "book", "2");
        SingleDocumentSample.getDocument(client, articleIndex, "article", "1");

        SearchSample.multiSearch(client, "example*", "lucene");
        SearchSample.matchQuery(client, bookIndex, "desc", "full-text");
        SearchSample.booleanQuery(client, bookIndex);
        SearchSample.fuzzyLikeQuery(client, bookIndex);
        SearchSample.matchAllQuery(client, bookIndex);
        SearchSample.prefixQuery(client, articleIndex);
        SearchSample.queryString(client, articleIndex);
        SearchSample.rangeQuery(client, articleIndex);
        SearchSample.termsQuery(client, articleIndex);
        SearchSample.wildcardQuery(client, articleIndex);
        SearchSample.regexpQuery(client, articleIndex);

        SingleDocumentSample.deleteDocument(client, bookIndex, "book", "1");
        IndicesSample.deleteIndices(client, bookIndex);
        IndicesSample.deleteIndices(client, articleIndex);
        IndicesSample.deleteIndices(client, tweetIndex);
        client.close();

        // 脚本esTransportClient.sh调用时正常结束必须exit(0)
        System.exit(0);
    }
}
