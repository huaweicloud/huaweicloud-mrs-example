/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.bigdata.spark.examples

import org.apache.http.HttpStatus
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.client.{Request, RequestOptions, Response, RestClient, RestHighLevelClient}
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse, SearchScrollRequest}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.hwclient.HwRestClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.{Scroll, SearchHit}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark
import org.slf4j.LoggerFactory

import java.io.{File, FileInputStream, IOException}
import java.util
import java.util.{Date, Locale, Properties}
import java.util.concurrent.ThreadLocalRandom

/**
 * The demo of Spark operates the Elasticsearch.
 * Must add the Elasticsearch-Spark jar into the dependencies.
 *
 * @since 2020-06-28
 */
class SparkOnEs {
  private val LOG = LoggerFactory.getLogger(classOf[SparkOnEs])

  private var esServerHost: String = _
  private var snifferEnable: String = _
  private var connectTimeout: Int = 0
  private var socketTimeout: Int = 0
  private var maxConnTotal: Int = 0
  private var maxConnPerRoute: Int = 0
  private var maxRetryTimeoutMillis: Int = 0
  private var connectionRequestTimeout: Int = 0

  private var isSecureMode: String = _
  private var sslEnabled: String = _

  private var esFilterField: String = _
  private var esScrollSize: String = _
  private var esInputMaxDocsPerPartition: String = _

  private var esIndex: String = _
  private var esShardNum: Int = 0
  private var esReplicaNum: Int = 0

  private var esQueryField: String = _
  private var esGroupField: String = _
  private var esQueryRangeBegin: String = _
  private var esQueryRangeEnd: String = _
  private var esQueryJsonString: String = _

  private var restClient: RestClient = _
  private var highLevelClient: RestHighLevelClient = _

  /*
   * Initialize basic configurations for Elasticsearch
   */
  @throws[IOException]
  def initProperties(): Unit = {
    val properties = new Properties
    var path = System.getProperty("user.dir") + File.separator + "esParams.properties"
    path = path.replace("\\", "\\\\")
    try {
      properties.load(new FileInputStream(new File(path)))
    } catch {
      case e: IOException =>
        throw new IOException("***** Failed to load properties file : " + path)
    }

    // esServerHost in esParams.properties must as ip1:port1,ip2:port2,ip3:port3....
    esServerHost = properties.getProperty("esServerHost")
    snifferEnable = properties.getProperty("snifferEnable")
    connectTimeout = properties.getProperty("connectTimeout").toInt
    socketTimeout = properties.getProperty("socketTimeout").toInt
    maxConnTotal = properties.getProperty("maxConnTotal").toInt
    maxConnPerRoute = properties.getProperty("maxConnPerRoute").toInt
    maxRetryTimeoutMillis = properties.getProperty("maxRetryTimeoutMillis").toInt
    connectionRequestTimeout = properties.getProperty("connectionRequestTimeout").toInt

    isSecureMode = properties.getProperty("isSecureMode")
    sslEnabled = properties.getProperty("sslEnabled")

    esFilterField = properties.getProperty("esFilterField")
    esScrollSize = properties.getProperty("esScrollSize")
    esInputMaxDocsPerPartition = properties.getProperty("esInputMaxDocsPerPartition")
    esIndex = properties.getProperty("esIndex")
    esShardNum = properties.getProperty("esShardNum").toInt
    esReplicaNum = properties.getProperty("esReplicaNum").toInt

    esGroupField = properties.getProperty("esGroupField")
    esQueryField = properties.getProperty("esQueryField")
    esQueryRangeBegin = properties.getProperty("esQueryRangeBegin")
    esQueryRangeEnd = properties.getProperty("esQueryRangeEnd")
    esQueryJsonString = String.format(
      Locale.ENGLISH,
      properties.getProperty("esQueryJsonString"),
      esQueryField,
      esQueryRangeBegin,
      esQueryRangeEnd)

    LOG.info("***** esServerHost: {}", esServerHost)
    LOG.info("***** snifferEnable: {}", snifferEnable)
    LOG.info("***** connectTimeout: {}", connectTimeout)
    LOG.info("***** socketTimeout: {}", socketTimeout)
    LOG.info("***** maxConnTotal: {}", maxConnTotal)
    LOG.info("***** maxConnPerRoute: {}", maxConnPerRoute)
    LOG.info("***** maxRetryTimeoutMillis: {}", maxRetryTimeoutMillis)
    LOG.info("***** connectionRequestTimeout: {}", connectionRequestTimeout)

    LOG.info("***** isSecureMode: {}", isSecureMode)
    LOG.info("***** sslEnabled: {}", sslEnabled)

    LOG.info("***** esFilterField: {}", esFilterField)
    LOG.info("***** esScrollSize: {}", esScrollSize)
    LOG.info("***** esInputMaxDocsPerPartition: {}", esInputMaxDocsPerPartition)

    LOG.info("***** esIndex: {}", esIndex)
    LOG.info("***** esShardNum: {}", esShardNum)
    LOG.info("***** esReplicaNum: {}", esReplicaNum)
    LOG.info("***** esGroupField: {}", esGroupField)
    LOG.info("***** esQueryField: {}", esQueryField)
    LOG.info("***** esQueryJsonString: {}", esQueryJsonString)
  }

  /**
   * Check whether the index has already existed in ES
   *
   * @param index the name of index for check
   */
  def exist(index: String): Boolean = {
    var response: Response = null
    try {
      val request = new Request("HEAD", "/" + index)
      response = restClient.performRequest(request)
      if (HttpStatus.SC_OK == response.getStatusLine.getStatusCode) {
        LOG.info("***** Check index successful, index is exist : {}", index)
        return true
      }
      if (HttpStatus.SC_NOT_FOUND == response.getStatusLine.getStatusCode) {
        LOG.info("***** Index is not exist: {}", index)
        return false
      }
    } catch {
      case e: Exception =>
        LOG.error("***** Check index failed, exception occurred.", e)
    }
    false
  }

  /**
   * Delete one index
   *
   * @param index the name of index for delete
   */
  def deleteIndex(index: String): Unit = {
    var response: Response = null
    try {
      val request = new Request("DELETE", "/" + index + "?pretty=true")
      response = restClient.performRequest(request)
      if (HttpStatus.SC_OK == response.getStatusLine.getStatusCode) LOG.info("***** Delete successful.")
      else LOG.error("***** Delete failed.")
      LOG.info("***** Delete response entity is: \n{}", EntityUtils.toString(response.getEntity))
    } catch {
      case e: Exception =>
        LOG.error("***** Delete failed, exception occurred.", e)
    }
  }

  /**
   * Create one index with shard number and replica number.
   *
   * @param index the name of index for create
   */
  def createIndex(index: String): Unit = {
    var response: Response = null
    val jsonString = "{" + "\"settings\":{" + "\"number_of_shards\":\"" + esShardNum + "\"," + "\"number_of_replicas\":\"" + esReplicaNum + "\"" + "}}"
    val entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON)
    try {
      val request = new Request("PUT", "/" + index)
      request.addParameter("pretty", "true")
      request.setEntity(entity)
      response = restClient.performRequest(request)
      if (HttpStatus.SC_OK == response.getStatusLine.getStatusCode) LOG.info("***** CreateIndex successful.")
      else LOG.error("***** CreateIndex failed.")
      LOG.info("***** CreateIndex response entity is: \n{}", EntityUtils.toString(response.getEntity))
    } catch {
      case e: Exception =>
        LOG.error("***** CreateIndex failed, exception occurred.", e)
    }
  }

  /**
   * Put data by a bulk request
   *
   * @param highLevelClient the Client of Elasticsearch
   */
  def putDataByBulk(highLevelClient: RestHighLevelClient): Unit = {
    // total number of documents need to index
    val totalRecordNum = 1000
    // number of document per bulk request
    val onceCommit = 500
    val circleNumber = totalRecordNum / onceCommit
    val esMap = new util.HashMap[String, Any]()

    for (i <- 0 until circleNumber) {
      val request = new BulkRequest()
      var id: String = null
      for (j <- 1 to onceCommit) {
        esMap.clear()
        id = i * onceCommit + j + ""
        esMap.put("id", id)
        esMap.put("name", "name-" + id)
        esMap.put("age", ThreadLocalRandom.current().nextInt(1, 30))
        esMap.put("createdTime", new Date())
        request.add(new IndexRequest(esIndex).source(esMap))
      }

      try {
        val response = highLevelClient.bulk(request, RequestOptions.DEFAULT)
        if (RestStatus.OK.equals(response.status)) LOG.info("***** Already input documents: {}", onceCommit * (i + 1))
        else LOG.error("***** Bulk failed.")
        LOG.info("***** Bulk request took: {} ms", response.getTook)
      } catch {
        case e: Exception =>
          LOG.error("***** Bulk failed, exception occurred.", e)
      }
    }
  }

  /**
   * Put data from HDFS into the ES index
   *
   * @param jsc the Java Spark Context which has already initialization
   */
  def putHdfsData(jsc: JavaSparkContext): Unit = {
    // Reading data from HDFS
    val inputs = jsc.textFile("/user/spark-on-es/people.json")
    val jsonList = inputs.collect
    LOG.info("***** Load data from HDFS successful, total: [{}]", jsonList.size)

    val rdd = jsc.parallelize(jsonList)
    JavaEsSpark.saveJsonToEs(rdd, esIndex)
    LOG.info("***** Put HDFS data to es successful.")
  }

  /**
   * Query data from ES by Spark Dataset
   *
   * @param conf the Spark Conf
   */
  def queryDataByDataset(conf: SparkConf): Unit = {
    val spark = SparkSession.builder.config(conf).getOrCreate
    val options = new util.HashMap[String, String]
    // you can set query condition in options() method
    options.put("es.query", esQueryJsonString)

    val begin: Long = System.currentTimeMillis
    val dataset = spark.read.format("org.elasticsearch.spark.sql").options(options).load(esIndex)
    val end: Long = System.currentTimeMillis
    val spentTime = end - begin

    LOG.info("***** Query data from ES by Spark Dataset, dataset's count: {}, spent time: {} ms",
      dataset.count, spentTime)
  }

  /**
   * Query data from ES by ES rest client
   *
   * @param highLevelClient the ES Rest High Level Client
   * @param jsc the Java Spark Context which has already initialization
   */
  def queryDataByRestClient(highLevelClient: RestHighLevelClient, jsc: JavaSparkContext): Unit = {
    val begin: Long = System.currentTimeMillis
    val resultMap: util.List[util.Map[String, AnyRef]] = new util.ArrayList[util.Map[String, AnyRef]](1024 * 100)

    try {
      // query data by scroll api, avoid the OOM
      val searchRequest: SearchRequest = new SearchRequest(esIndex)
      val scroll: Scroll = new Scroll(TimeValue.timeValueMinutes(1L))
      searchRequest.scroll(scroll)

      // set the size of result, note: if the number of size was too large, may cause the OOM
      val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder().size(2000)
      searchSourceBuilder.query(QueryBuilders.rangeQuery(esQueryField).gte(esQueryRangeBegin).lt(esQueryRangeEnd))
      val includeFields: Array[String] = Array[String]("id", "name", "birthday")
      val excludeFields: Array[String] = Array[String]("age")
      searchSourceBuilder.fetchSource(includeFields, excludeFields)
      searchRequest.source(searchSourceBuilder)
      var searchResponse: SearchResponse = highLevelClient.search(searchRequest, RequestOptions.DEFAULT)
      var scrollId: String = searchResponse.getScrollId
      var searchHits: Array[SearchHit] = searchResponse.getHits.getHits
      while (searchHits != null && searchHits.length > 0) {
        for (hit <- searchHits) {
          val source: util.Map[String, AnyRef] = hit.getSourceAsMap
          resultMap.add(source)
        }

        // continue scroll search
        val scrollRequest: SearchScrollRequest = new SearchScrollRequest(scrollId)
        scrollRequest.scroll(scroll)
        searchResponse = highLevelClient.scroll(scrollRequest, RequestOptions.DEFAULT)
        scrollId = searchResponse.getScrollId
        searchHits = searchResponse.getHits.getHits
      }
    } catch {
      case e: Exception =>
        LOG.error("***** Query data failed, exception occurred.", e)
    }

    val rdd: JavaRDD[util.Map[String, AnyRef]] = jsc.parallelize(resultMap)

    val end: Long = System.currentTimeMillis
    val spentTime: Long = end - begin
    LOG.info("***** Query data from ES by Rest High Level Client, rdd's count: {}, spent time: {} ms",
      rdd.count, spentTime)
  }

  /**
   * 1. query all data from ES by JavaEsSpark,
   * 2. group by specified field and sort the result,
   * 3. save them to HDFS or local files
   *
   * @param sc the Java Spark Context which has already initialization
   */
  def groupBySpark(sc: SparkContext): Unit = {
    val begin = System.currentTimeMillis
    val pairRDD = EsSpark.esRDD(sc, esIndex)

    val field = esGroupField
    val valueRDD = pairRDD
      .mapPartitions(itr => {
        itr.map(row => (row._2(field), 1L))
      }, preservesPartitioning = true)
      .reduceByKey(_ + _)
      .sortBy(doc => doc._2, ascending = false)

    val end = System.currentTimeMillis
    val spentTime = end - begin
    LOG.info("***** GroupBy data from ES successful, spent time: {} ms", spentTime)

    valueRDD.saveAsTextFile("/user/spark-on-es/group-result/")
    LOG.info("***** Save all result to HDFS successful.")
  }

  /**
   * Close resources before the program terminates.
   */
  def closeResources(): Unit = {
    try {
      if (restClient != null) {
        restClient.close()
        LOG.info("***** Close the Rest Client successful.")
      }
      if (highLevelClient != null) {
        highLevelClient.close()
        LOG.info("***** Close the Rest High Level Client successful.")
      }
    } catch {
      case e: IOException =>
        LOG.error("***** Close the resource failed.", e)
    }
  }
}

object SparkOnEs {
  def main(args: Array[String]): Unit = {
    val sparkOnEs = new SparkOnEs
    sparkOnEs.LOG.info("***** Start to run the Spark on ES test.")

    try {
      // init the global properties
      sparkOnEs.initProperties()

      val path = System.getProperty("user.dir") + File.separator
      val hwRestClient: HwRestClient = new HwRestClient(path)
      sparkOnEs.restClient = hwRestClient.getRestClient
      sparkOnEs.highLevelClient = new RestHighLevelClient(hwRestClient.getRestClientBuilder)

      // Check whether the index to be added has already exist, remove the exist one
      if (sparkOnEs.exist(sparkOnEs.esIndex)) sparkOnEs.deleteIndex(sparkOnEs.esIndex)
      sparkOnEs.createIndex(sparkOnEs.esIndex)

      // Put data by ES bulk request
      sparkOnEs.putDataByBulk(sparkOnEs.highLevelClient)

      // Create a configuration class SparkConf,
      // meanwhile set the Secure configuration that the Elasticsearch Cluster needed,
      // finally create a SparkContext.
      val conf: SparkConf = new SparkConf().setAppName("SparkOnEs")
        .set("es.nodes", sparkOnEs.esServerHost)
        // when you specified in es.nodes, then es.port is not necessary
        // .set("es.port", "24100")
        .set("es.nodes.discovery", "true")
        .set("es.index.auto.create", "true")
        .set("es.internal.spark.sql.pushdown", "true")
        .set("es.read.source.filter", sparkOnEs.esFilterField)
        .set("es.scroll.size", sparkOnEs.esScrollSize)
        .set("es.input.max.docs.per.partition", sparkOnEs.esInputMaxDocsPerPartition)

      val jsc: JavaSparkContext = new JavaSparkContext(conf)

      sparkOnEs.putHdfsData(jsc) // Put data from HDFS into ES
      sparkOnEs.queryDataByDataset(conf) // Query data from ES
      sparkOnEs.queryDataByRestClient(sparkOnEs.highLevelClient, jsc)
      sparkOnEs.groupBySpark(jsc) // Group data from ES

      jsc.close()
    } catch {
      case e: IOException =>
        sparkOnEs.LOG.error("***** There are exceptions in main.", e)
    } finally {
      sparkOnEs.closeResources()
    }
  }
}
