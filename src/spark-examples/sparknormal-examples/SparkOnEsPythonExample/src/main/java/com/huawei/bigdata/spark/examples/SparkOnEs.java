/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.bigdata.spark.examples;

import scala.Tuple2;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.hwclient.HwRestClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The demo of Spark operates the Elasticsearch
 *
 * @apiNote Must add the Elasticsearch-Spark jar into the dependencies
 * @since 2020-06-28
 */
public class SparkOnEs implements Serializable {
    private static final long serialVersionUID = -814943961767553483L;
    private static final Logger LOG = LoggerFactory.getLogger(SparkOnEs.class);

    private static String esServerHost;
    private static String snifferEnable;
    private static int connectTimeout;
    private static int socketTimeout;
    private static int maxConnTotal;
    private static int maxConnPerRoute;
    private static int maxRetryTimeoutMillis;
    private static int connectionRequestTimeout;

    private static String isSecureMode;
    private static String sslEnabled;

    private static String esFilterField;
    private static String esScrollSize;
    private static String esInputMaxDocsPerPartition;

    private static String esIndex;
    private static int esShardNum;
    private static int esReplicaNum;

    private static String esQueryField;
    private static String esGroupField;
    private static String esQueryRangeBegin;
    private static String esQueryRangeEnd;
    private static String esQueryJsonString;

    private static RestClient restClient;
    private static RestHighLevelClient highLevelClient;

    /**
     * The entry for PySpark call
     */
    public void main(JavaSparkContext jsc) {
        LOG.info("***** Start to run the Spark on ES test.");

        try {
            // init the global properties
            initProperties();

            String path = System.getProperty("user.dir") + File.separator;
            HwRestClient hwRestClient = new HwRestClient(path);
            restClient = hwRestClient.getRestClient();
            highLevelClient = new RestHighLevelClient((hwRestClient.getRestClientBuilder()));

            // Check whether the index to be added has already exist, remove the exist one
            if (exist(esIndex)) {
                deleteIndex(esIndex);
            }
            createIndex(esIndex);

            SparkOnEs instance = new SparkOnEs();
            instance.putDataByBulk(highLevelClient); // Put data by ES bulk request
            instance.putHdfsData(jsc); // Put data from HDFS into ES
            instance.queryDataByDataset(jsc.getConf()); // Query data from ES
            instance.queryDataByRestClient(highLevelClient, jsc);
            instance.groupBySpark(jsc); // Group data from ES

            jsc.stop();
        } catch (IOException e) {
            LOG.error("***** There are exceptions in main.", e);
        } finally {
            closeResources();
        }
    }

    /**
     * Close resources before the program terminates.
     */
    public static void closeResources() {
        try {
            if (restClient != null) {
                restClient.close();
                LOG.info("***** Close the Rest Client successful.");
            }
            if (highLevelClient != null) {
                highLevelClient.close();
                LOG.info("***** Close the Rest High Level Client successful.");
            }
        } catch (IOException e) {
            LOG.error("***** Close the resource failed.", e);
        }
    }

    /**
     * 1. query all data from ES by JavaEsSpark,
     * 2. group by specified field and sort the result,
     * 3. save the result to HDFS or local files
     *
     * @param jsc the Java Spark Context which has already initialization
     */
    public void groupBySpark(JavaSparkContext jsc) {
        long begin = System.currentTimeMillis();
        JavaPairRDD<String, Map<String, Object>> pairRDD = JavaEsSpark.esRDD(jsc, esIndex);

        final String field = esGroupField;
        // group by the esGroupField
        JavaPairRDD<String, Long> resultRdd =
                pairRDD.mapPartitionsToPair(
                                new PairFlatMapFunction<Iterator<Tuple2<String, Map<String, Object>>>, String, Long>() {
                                    @Override
                                    public Iterator<Tuple2<String, Long>> call(
                                            Iterator<Tuple2<String, Map<String, Object>>> iterator) throws Exception {
                                        List<Tuple2<String, Long>> list = new ArrayList<>(10000);
                                        iterator.forEachRemaining(
                                                row -> list.add(new Tuple2<>(row._2.get(field).toString(), 1L)));
                                        return list.iterator();
                                    }
                                })
                        .reduceByKey((v1, v2) -> (v1 + v2))
                        .mapToPair(row -> new Tuple2<>(row._2, row._1))
                        // sort by the esFilterField's times
                        .sortByKey(false)
                        .mapToPair(row -> new Tuple2<>(row._2, row._1));

        long end = System.currentTimeMillis();
        long spentTime = end - begin;
        LOG.info("***** GroupBy data from ES successful, spent time: {} ms", spentTime);

        resultRdd.saveAsTextFile("/user/spark-on-es/group-result/");
        LOG.info("***** Save all result to HDFS successful.");
    }

    /**
     * Query data from ES by ES rest client
     *
     * @param highLevelClient the ES Rest High Level Client
     * @param jsc the Java Spark Context which has already initialization
     */
    public void queryDataByRestClient(RestHighLevelClient highLevelClient, JavaSparkContext jsc) {
        long begin = System.currentTimeMillis();
        List<Map<String, Object>> resultMap = new ArrayList<>(1024 * 100);

        try {
            // query data by scroll api, avoid the OOM
            SearchRequest searchRequest = new SearchRequest(esIndex);
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            searchRequest.scroll(scroll);

            // set the size of result, note: if the number of size was too large, may cause the OOM
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(Integer.parseInt(esScrollSize));
            searchSourceBuilder.query(
                    QueryBuilders.rangeQuery(esQueryField).gte(esQueryRangeBegin).lt(esQueryRangeEnd));
            String[] includeFields = new String[] {"id", "name", "birthday"};
            String[] excludeFields = new String[] {"age"};
            searchSourceBuilder.fetchSource(includeFields, excludeFields);

            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT);

            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit hit : searchHits) {
                    Map<String, Object> source = hit.getSourceAsMap();
                    resultMap.add(source);
                }

                // continue scroll search
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = highLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
        } catch (IOException e) {
            LOG.error("***** Query data failed, exception occurred.", e);
        }

        JavaRDD<Map<String, Object>> rdd = jsc.parallelize(resultMap);

        long end = System.currentTimeMillis();
        long spentTime = end - begin;
        LOG.info(
                "***** Query data from ES by Rest High Level Client, rdd's count: {}, spent time: {} ms",
                rdd.count(),
                spentTime);
    }

    /**
     * Query data from ES by Spark Dataset
     *
     * @param conf the Spark Conf
     */
    public void queryDataByDataset(SparkConf conf) {
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        Map<String, String> options = new HashMap<>();
        // you can set query condition in options() method
        options.put("es.query", esQueryJsonString);

        long begin = System.currentTimeMillis();
        Dataset<Row> dataset = spark.read().format("org.elasticsearch.spark.sql").options(options).load(esIndex);

        long end = System.currentTimeMillis();
        long spentTime = end - begin;
        LOG.info(
                "***** Query data from ES by Spark Dataset, dataset's count: {}, spent time: {} ms",
                dataset.count(),
                spentTime);
    }

    /**
     * Put data from HDFS into the ES index
     *
     * @param jsc the Java Spark Context which has already initialization
     */
    public void putHdfsData(JavaSparkContext jsc) {
        // Reading data from HDFS
        JavaRDD<String> inputs = jsc.textFile("/user/spark-on-es/people.json");
        List<String> jsonList = inputs.collect();
        LOG.info("***** Load data from HDFS successful, total: [{}]", jsonList.size());

        JavaRDD<String> rdd = jsc.parallelize(jsonList);
        JavaEsSpark.saveJsonToEs(rdd, esIndex);
        LOG.info("***** Put HDFS data to ES successful.");
    }

    /**
     * Put data by a bulk request
     *
     * @param highLevelClient the Client of Elasticsearch
     */
    public void putDataByBulk(RestHighLevelClient highLevelClient) {
        // total number of documents need to index
        long totalRecordNum = 1000;
        // number of documents per bulk request
        long onceCommit = 500;
        long circleNumber = totalRecordNum / onceCommit;
        Map<String, Object> esMap = new HashMap<>();

        for (int i = 0; i < circleNumber; i++) {
            BulkRequest request = new BulkRequest();
            String id;
            for (int j = 1; j <= onceCommit; j++) {
                esMap.clear();
                id = i * onceCommit + j + "";
                esMap.put("id", id);
                esMap.put("name", "name-" + id);
                esMap.put("age", ThreadLocalRandom.current().nextInt(1, 30));
                esMap.put("createdTime", new Date());
                request.add(new IndexRequest(esIndex).source(esMap));
            }

            try {
                BulkResponse response = highLevelClient.bulk(request, RequestOptions.DEFAULT);
                if (RestStatus.OK.equals(response.status())) {
                    LOG.info("***** Already input documents: {}", onceCommit * (i + 1));
                } else {
                    LOG.error("***** Bulk failed.");
                }
                LOG.info("***** Bulk request took: {} ms", response.getTook());
            } catch (IOException e) {
                LOG.error("***** Bulk failed, exception occurred.", e);
            }
        }
    }

    /**
     * Create one index with shard number and replica number.
     *
     * @param index the name of index for create
     */
    public static void createIndex(String index) {
        Response response;
        String jsonString =
                "{"
                        + "\"settings\":{"
                        + "\"number_of_shards\":\""
                        + esShardNum
                        + "\","
                        + "\"number_of_replicas\":\""
                        + esReplicaNum
                        + "\""
                        + "}}";

        HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
        try {
            Request request = new Request("PUT", "/" + index);
            request.addParameter("pretty", "true");
            request.setEntity(entity);
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("***** CreateIndex successful.");
            } else {
                LOG.error("***** CreateIndex failed.");
            }
            LOG.info("***** CreateIndex response entity is: \n{}", EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            LOG.error("***** CreateIndex failed, exception occurred.", e);
        }
    }

    /**
     * Delete one index
     *
     * @param index the name of index for delete
     */
    public static void deleteIndex(String index) {
        Response response;
        try {
            Request request = new Request("DELETE", "/" + index + "?pretty=true");
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("***** DeleteIndex successful.");
            } else {
                LOG.error("***** DeleteIndex failed.");
            }
            LOG.info("***** DeleteIndex response entity is: \n{}", EntityUtils.toString(response.getEntity()));
        } catch (IOException e) {
            LOG.error("***** DeleteIndex failed, exception occurred.", e);
        }
    }

    /**
     * Check whether the index has already existed in ES
     *
     * @param index the name of index for check
     */
    public static boolean exist(String index) {
        Response response;
        try {
            Request request = new Request("HEAD", "/" + index);
            response = restClient.performRequest(request);
            if (HttpStatus.SC_OK == response.getStatusLine().getStatusCode()) {
                LOG.info("***** Check index successful, index is exist : {}", index);
                return true;
            }
            if (HttpStatus.SC_NOT_FOUND == response.getStatusLine().getStatusCode()) {
                LOG.info("***** Index is not exist: {}", index);
                return false;
            }
        } catch (IOException e) {
            LOG.error("***** Check index failed, exception occurred.", e);
        }
        return false;
    }

    /*
     * Initialize basic configurations for Elasticsearch
     */
    public static void initProperties() throws IOException {
        Properties properties = new Properties();
        String path = System.getProperty("user.dir") + File.separator + "esParams.properties";
        path = path.replace("\\", "\\\\");

        try {
            properties.load(new FileInputStream(new File(path)));
        } catch (IOException e) {
            throw new IOException("***** Failed to load properties file: " + path);
        }

        // esServerHost in esParams.properties must as ip1:port1,ip2:port2,ip3:port3....
        esServerHost = properties.getProperty("esServerHost");
        snifferEnable = properties.getProperty("snifferEnable");
        connectTimeout = Integer.parseInt(properties.getProperty("connectTimeout"));
        socketTimeout = Integer.parseInt(properties.getProperty("socketTimeout"));
        maxConnTotal = Integer.parseInt(properties.getProperty("maxConnTotal"));
        maxConnPerRoute = Integer.parseInt(properties.getProperty("maxConnPerRoute"));
        maxRetryTimeoutMillis = Integer.parseInt(properties.getProperty("maxRetryTimeoutMillis"));
        connectionRequestTimeout = Integer.parseInt(properties.getProperty("connectionRequestTimeout"));

        isSecureMode = properties.getProperty("isSecureMode");
        sslEnabled = properties.getProperty("sslEnabled");

        esFilterField = properties.getProperty("esFilterField");
        esScrollSize = properties.getProperty("esScrollSize");
        esInputMaxDocsPerPartition = properties.getProperty("esInputMaxDocsPerPartition");
        esIndex = properties.getProperty("esIndex");
        esShardNum = Integer.parseInt(properties.getProperty("esShardNum"));
        esReplicaNum = Integer.parseInt(properties.getProperty("esReplicaNum"));

        esGroupField = properties.getProperty("esGroupField");
        esQueryField = properties.getProperty("esQueryField");
        esQueryRangeBegin = properties.getProperty("esQueryRangeBegin");
        esQueryRangeEnd = properties.getProperty("esQueryRangeEnd");
        esQueryJsonString =
                String.format(
                        Locale.ENGLISH,
                        properties.getProperty("esQueryJsonString"),
                        esQueryField,
                        esQueryRangeBegin,
                        esQueryRangeEnd);

        LOG.info("***** esServerHost: {}", esServerHost);
        LOG.info("***** snifferEnable: {}", snifferEnable);
        LOG.info("***** connectTimeout: {}", connectTimeout);
        LOG.info("***** socketTimeout: {}", socketTimeout);
        LOG.info("***** maxConnTotal: {}", maxConnTotal);
        LOG.info("***** maxConnPerRoute: {}", maxConnPerRoute);
        LOG.info("***** maxRetryTimeoutMillis: {}", maxRetryTimeoutMillis);
        LOG.info("***** connectionRequestTimeout: {}", connectionRequestTimeout);

        LOG.info("***** isSecureMode: {}", isSecureMode);
        LOG.info("***** sslEnabled: {}", sslEnabled);

        LOG.info("***** esFilterField: {}", esFilterField);
        LOG.info("***** esScrollSize: {}", esScrollSize);
        LOG.info("***** esInputMaxDocsPerPartition: {}", esInputMaxDocsPerPartition);

        LOG.info("***** esIndex: {}", esIndex);
        LOG.info("***** esShardNum: {}", esShardNum);
        LOG.info("***** esReplicaNum: {}", esReplicaNum);
        LOG.info("***** esGroupField: {}", esGroupField);
        LOG.info("***** esQueryField: {}", esQueryField);
        LOG.info("***** esQueryJsonString: {}", esQueryJsonString);
    }
}
