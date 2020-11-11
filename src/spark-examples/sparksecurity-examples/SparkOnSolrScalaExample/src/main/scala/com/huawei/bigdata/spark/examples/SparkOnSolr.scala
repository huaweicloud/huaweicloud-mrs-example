package com.huawei.bigdata.spark.examples

import java.io.{File, FileInputStream, IOException}
import java.util
import java.util._
import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.client.solrj.request.{CollectionAdminRequest, UpdateRequest}
import org.apache.solr.client.solrj.response.CollectionAdminResponse
import org.apache.solr.client.solrj.{SolrQuery, SolrServerException}
import org.apache.solr.common.SolrInputDocument
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.ListBuffer

class SparkOnSolr {
  private val LOG = LoggerFactory.getLogger(classOf[SparkOnSolr])
  private var solrKbsEnable : Boolean = false //whether is security mode
  private var zkClientTimeout : Int= 0
  private var zkConnectTimeout : Int = 0
  private var zkHost : String= null
  private var zookeeperDefaultServerPrincipal : String = null //default user to access zookeeper  in that Solr is based on ZK.

  private var collectionName : String = null //the name of collection to be created

  private var defaultConfigName : String = null
  private var shardNum : Int= 0
  private var replicaNum : Int= 0
  private var principal : String= null
  private var sharedFsReplication : java.lang.Boolean= false
  private var maxShardsPerNode : Int = 0
  private var autoAddReplicas : Boolean = false
  private var assignToSpecifiedNodeSet : Boolean = false
  private var createNodeSet : String = null


  @throws[SolrException]
  private def initProperties() = {
    val properties = new Properties
    var proPath = System.getProperty("user.dir") + File.separator + "solr-example.properties"
    proPath = proPath.replace("\\", "\\\\")
    try
      properties.load(new FileInputStream(new File(proPath)))
    catch {
      case e: IOException =>
        throw new SolrException("Failed to load properties file : " + proPath, null)
    }
    System.out.println("load property file successfully!")
    solrKbsEnable = properties.getProperty("SOLR_KBS_ENABLED").toBoolean
    zkClientTimeout = Integer.valueOf(properties.getProperty("zkClientTimeout"))
    zkConnectTimeout = Integer.valueOf(properties.getProperty("zkConnectTimeout"))
    zkHost = properties.getProperty("zkHost")
    zookeeperDefaultServerPrincipal = properties.getProperty("ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL")
    collectionName = properties.getProperty("COLLECTION_NAME")
    defaultConfigName = properties.getProperty("DEFAULT_CONFIG_NAME")
    shardNum = Integer.valueOf(properties.getProperty("shardNum"))
    replicaNum = Integer.valueOf(properties.getProperty("replicaNum"))
    principal = properties.getProperty("principal")
    maxShardsPerNode = Integer.valueOf(properties.getProperty("maxShardsPerNode"))
    autoAddReplicas = properties.getProperty("autoAddReplicas").toBoolean
    sharedFsReplication = if (properties.getProperty("sharedFsReplication") == null) null
    else properties.getProperty("sharedFsReplication").toBoolean
    assignToSpecifiedNodeSet = properties.getProperty("assignToSpecifiedNodeSet").toBoolean
    createNodeSet = properties.getProperty("createNodeSet")
  }


  @throws[SolrException]
  private def getCloudSolrClient(zkHost: String) = {
    val builder = new CloudSolrClient.Builder
    builder.withZkHost(zkHost)
    val cloudSolrClient = builder.build
    cloudSolrClient.setZkClientTimeout(zkClientTimeout)
    cloudSolrClient.setZkConnectTimeout(zkConnectTimeout)
    cloudSolrClient.connect()
    LOG.info("The cloud Server has been connected !!!!")
    val zkStateReader = cloudSolrClient.getZkStateReader
    val cloudState = zkStateReader.getClusterState
    LOG.info("The zookeeper state is : {}", cloudState)
    cloudSolrClient
  }

  @throws[SolrException]
  private def queryIndex(cloudSolrClient: CloudSolrClient) = {
    val query = new SolrQuery
    query.setQuery("name:Luna*")
    try {
      val response = cloudSolrClient.query(query)
      val docs = response.getResults
      LOG.info("Query wasted time : {}ms", response.getQTime)
      val resultList = ListBuffer[ListBuffer[String]]()
      LOG.info("Total doc num : {}", docs.getNumFound)
      import scala.collection.JavaConversions._
      for (doc <- docs) {
        LOG.info("doc detail : " + doc.getFieldValueMap)
        resultList.append(transferMapToList(doc.getFieldValueMap))
      }
      resultList
    } catch {
      case e: SolrServerException =>
        LOG.error("Failed to query document", e)
        throw new SolrException("Failed to query document", null)
      case e: IOException =>
        LOG.error("Failed to query document", e)
        throw new SolrException("Failed to query document", null)
      case e: Exception =>
        LOG.error("Failed to query document", e)
        throw new SolrException("unknown exception", null)
    }
  }

  /*
  * Resolve the unserializable result to List<String>
  * */
  private def transferMapToList(rawMap: util.Map[String, AnyRef]) = {
    import scala.collection.JavaConversions._
    val tempList = ListBuffer[String]()
    for (key <- rawMap.keySet) {
      tempList.append(key + ":" + rawMap.get(key).toString)
    }
    tempList
  }

  @throws[SolrException]
  private def removeIndex(cloudSolrClient: CloudSolrClient) = {
    try {
      cloudSolrClient.deleteByQuery("*:*")
      LOG.info("Success to delete index")
    } catch {
      case e: SolrServerException =>
        LOG.error("Failed to remove document", e)
        throw new SolrException("Failed to remove document", null)
      case e: IOException =>
        LOG.error("Failed to remove document", e)
        throw new SolrException("Failed to remove document", null)
    }
  }

  @throws[SolrException]
  private def addDocs(cloudSolrClient: CloudSolrClient) = {
    var documents : scala.collection.mutable.Buffer[SolrInputDocument] = scala.collection.mutable.Buffer[SolrInputDocument]()
    var i = 0
    import collection.JavaConversions._
    while ( {
      i < 5
    }) {
      val doc = new SolrInputDocument
      doc.addField("id", i.toString)
      doc.addField("name", "Luna_" + i)
      doc.addField("features", "test" + i)
      doc.addField("price", i.asInstanceOf[Float] * 1.01)
      documents.+:=(doc)
      i += 1;
    }
    try {
      cloudSolrClient.add(documents)
      LOG.info("success to add index")
    } catch {
      case e: SolrServerException =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("Failed to add document to collection", null)
      case e: IOException =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("Failed to add document to collection", null)
      case e: Exception =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("unknown exception", null)
    }
  }

  @throws[SolrException]
  private def addDocs2(cloudSolrClient: CloudSolrClient) = {
    val request = new UpdateRequest
    import collection.JavaConversions._
    var documents : scala.collection.mutable.Buffer[SolrInputDocument] = scala.collection.mutable.Buffer[SolrInputDocument]()
    var i = 5
    while ( {
      i < 10
    }) {
      val doc = new SolrInputDocument
      doc.addField("id", i.toString)
      doc.addField("name", "Luna_" + i)
      doc.addField("features", "test" + i)
      doc.addField("price", i.asInstanceOf[Float] * 1.01)
      documents.+=:(doc)
      i+=1
    }
    request.add(documents)
    try
      cloudSolrClient.request(request)
    catch {
      case e: SolrServerException =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("Failed to add document to collection", null)
      case e: IOException =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("Failed to add document to collection", null)
      case e: Exception =>
        LOG.error("Failed to add document to collection", e)
        throw new SolrException("unknown exception", null)
    }
  }

  @throws[SolrException]
  private def createCollection(cloudSolrClient: CloudSolrClient) = {
    val create : CollectionAdminRequest.Create= CollectionAdminRequest.createCollection(collectionName, defaultConfigName, shardNum, replicaNum)
    create.setMaxShardsPerNode(maxShardsPerNode)
    create.setAutoAddReplicas(autoAddReplicas)
    if (assignToSpecifiedNodeSet) create.setCreateNodeSet(createNodeSet)
    var response : CollectionAdminResponse = null
    try
      response = create.process(cloudSolrClient)
    catch {
      case e: SolrServerException =>
        LOG.error("Failed to create collection", e)
        throw new SolrException("Failed to create collection", null)
      case e: IOException =>
        LOG.error("Failed to create collection", e)
        throw new SolrException("Failed to create collection", null)
      case e: Exception =>
        LOG.error("Failed to create collection", e)
        throw new SolrException("unknown exception", null)
    }
    if (response.isSuccess) LOG.info("Success to create collection[{}]", collectionName)
    else {
      LOG.error("Failed to create collection[{}], cause : {}", response.getErrorMessages)
      throw new SolrException("Failed to create collection", null)
    }
  }

  @throws[SolrException]
  private def deleteCollection(cloudSolrClient: CloudSolrClient) = {
    val delete : CollectionAdminRequest.Delete = CollectionAdminRequest.deleteCollection(collectionName)
    var response : CollectionAdminResponse = null
    try
      response = delete.process(cloudSolrClient)
    catch {
      case e: SolrServerException =>
        LOG.error("Failed to delete collection", e)
        throw new SolrException("Failed to create collection", null)
      case e: IOException =>
        LOG.error("Failed to delete collection", e)
        throw new SolrException("unknown exception", null)
      case e: Exception =>
        LOG.error("Failed to delete collection", e)
        throw new SolrException("unknown exception", null)
    }
    if (response.isSuccess) LOG.info("Success to delete collection[{}]", collectionName)
    else {
      LOG.error("Failed to delete collection[{}], cause : {}", response.getErrorMessages)
      throw new SolrException("Failed to delete collection", null)
    }
  }

  @SuppressWarnings(Array("unchecked"))
  @throws[SolrException]
  private def queryAllCollections(cloudSolrClient: CloudSolrClient) = {
    val list = new CollectionAdminRequest.List
    var listRes : CollectionAdminResponse = null
    try
      listRes = list.process(cloudSolrClient)
    catch {
      case e: SolrServerException =>
        LOG.error("Failed to list collection", e)
        throw new SolrException("Failed to list collection",null)
      case e: IOException =>
        LOG.error("Failed to list collection", e)
        throw new SolrException("Failed to list collection",null)
      case e: Exception =>
        LOG.error("Failed to list collection", e)
        throw new SolrException("unknown exception",null)
    }
    val collectionNames : List[String] = listRes.getResponse.get("collections").asInstanceOf[List[String]]
    LOG.info("All existed collections : {}", collectionNames)
    collectionNames
  }
}

object SparkOnSolr{
  def main(args: Array[String]): Unit ={
    val testSample = new SparkOnSolr
    testSample.initProperties()
    try {
      val userPrincipal = "super"
      val userKeytabPath = System.getProperty("user.dir") + File.separator + "user.keytab"
      val krb5ConfPath = System.getProperty("user.dir") + File.separator + "krb5.conf"
      // The following statement implys that you can use SolrClient section in jaas.conf
      System.setProperty("solr.kerberos.jaas.appname", "SolrClient")
      LoginUtil.setJaasFile(userPrincipal, userKeytabPath)
      LoginUtil.setZookeeperServerPrincipal(testSample.zookeeperDefaultServerPrincipal)
      val hadoopConf = new Configuration
      LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf)
    } catch {
      case e: Exception =>
        testSample.LOG.error("Exceptions occurred when try to login with keytab")
    }
    val conf = new SparkConf().setAppName("SparkOnSolr")
    val sc = new SparkContext(conf)
    var cloudSolrClient: CloudSolrClient = null
    try {
      cloudSolrClient = testSample.getCloudSolrClient(testSample.zkHost)
      val collectionNames = testSample.queryAllCollections(cloudSolrClient)
      //delete the collection if exists
      if (collectionNames.contains(testSample.collectionName)) testSample.deleteCollection(cloudSolrClient)
      testSample.createCollection(cloudSolrClient)
      cloudSolrClient.setDefaultCollection(testSample.collectionName)
      testSample.addDocs(cloudSolrClient)
      testSample.addDocs2(cloudSolrClient)
      //Some time is needed to build the index
      try
        Thread.sleep(2000)
      catch {
        case e: InterruptedException =>

      }
      val queryResult1 = testSample.queryIndex(cloudSolrClient)

      import collection.JavaConversions._
      val resultRDD = sc.parallelize(queryResult1)
      System.out.println("Before delete, the total count is:" + resultRDD.count + "\n")
      testSample.removeIndex(cloudSolrClient)
      //Some time is needed to delete the index
      try
        Thread.sleep(2000)
      catch {
        case e: InterruptedException =>

      }
      val queryResult2 = testSample.queryIndex(cloudSolrClient)
      val resultRDD2 = sc.parallelize(queryResult2)
      System.out.println("After delete, the total count is:" + resultRDD2.count + "\n")
    } catch {
      case e: SolrException =>
        throw new SolrException(e.getMessage, null)
    } finally if (cloudSolrClient != null) try
      cloudSolrClient.close()
    catch {
      case e: IOException =>
        testSample.LOG.warn("Failed to close cloudSolrClient", e)
    }
    sc.stop()
  }
}