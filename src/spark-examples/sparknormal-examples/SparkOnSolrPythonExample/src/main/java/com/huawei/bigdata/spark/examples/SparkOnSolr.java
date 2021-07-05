/*
 * Copyright Notice:
 *      Copyright  1998-2019, Huawei Technologies Co., Ltd.  ALL Rights Reserved.
 *
 *      Warning: This computer software sourcecode is protected by copyright law
 *      and international treaties. Unauthorized reproduction or distribution
 *      of this sourcecode, or any portion of it, may result in severe civil and
 *      criminal penalties, and will be prosecuted to the maximum extent
 *      possible under the law.
 */
package com.huawei.bigdata.spark.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient.Builder;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class SparkOnSolr {
    private static final Logger LOG = LoggerFactory.getLogger(SparkOnSolr.class);
    private static boolean solrKbsEnable; // whether is security mode
    private static int zkClientTimeout;
    private static int zkConnectTimeout;
    private static String zkHost;
    private static String
            zookeeperDefaultServerPrincipal; // default user to access zookeeper  in that Solr is based on ZK.
    private static String collectionName; // the name of collection to be created
    private static String defaultConfigName;
    private static int shardNum;
    private static int replicaNum;
    private static String principal;
    private static Boolean sharedFsReplication;
    private static int maxShardsPerNode;
    private static boolean autoAddReplicas;
    private static boolean assignToSpecifiedNodeSet;
    private static String createNodeSet;

    public void sparkonsolr(JavaSparkContext jsc) throws Exception {
        initProperties();
        CloudSolrClient cloudSolrClient = null;
        try {
            cloudSolrClient = getCloudSolrClient(zkHost);

            List<String> collectionNames = queryAllCollections(cloudSolrClient);
            // delete the collection if exists
            if (collectionNames.contains(collectionName)) {
                deleteCollection(cloudSolrClient);
            }

            createCollection(cloudSolrClient);

            cloudSolrClient.setDefaultCollection(collectionName);

            addDocs(cloudSolrClient);

            addDocs2(cloudSolrClient);

            // Some time is needed to build the index
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

            List<Map<String, Object>> queryResult1 = queryIndex(cloudSolrClient);

            List<List<String>> resultList = new ArrayList<>();

            for (int i = 0; i < queryResult1.size(); i++) {
                resultList.add(transferMapToList(queryResult1.get(i)));
            }

            JavaRDD<List<String>> resultRDD = jsc.parallelize(resultList);

            System.out.println("Before delete, the total count is:" + resultRDD.count() + "\n");

            removeIndex(cloudSolrClient);

            // Some time is needed to delete the index
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }

            List<Map<String, Object>> queryResult2 = queryIndex(cloudSolrClient);

            System.out.println("After delete, the total count is:" + queryResult2.size() + "\n");

        } catch (SolrException e) {
            throw new SolrException(e.getMessage());
        } finally {
            if (cloudSolrClient != null) {
                try {
                    cloudSolrClient.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close cloudSolrClient", e);
                }
            }
        }
    }

    private void initProperties() throws SolrException {
        Properties properties = new Properties();
        String proPath = System.getProperty("user.dir") + File.separator + "solr-example.properties";
        proPath = proPath.replace("\\", "\\\\");
        try {
            properties.load(new FileInputStream(new File(proPath)));

        } catch (IOException e) {
            throw new SolrException("Failed to load properties file : " + proPath);
        }
        System.out.println("load property file successfully!");
        solrKbsEnable = Boolean.parseBoolean(properties.getProperty("SOLR_KBS_ENABLED"));
        zkClientTimeout = Integer.valueOf(properties.getProperty("zkClientTimeout"));
        zkConnectTimeout = Integer.valueOf(properties.getProperty("zkConnectTimeout"));
        zkHost = properties.getProperty("zkHost");
        zookeeperDefaultServerPrincipal = properties.getProperty("ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL");
        collectionName = properties.getProperty("COLLECTION_NAME");
        defaultConfigName = properties.getProperty("DEFAULT_CONFIG_NAME");
        shardNum = Integer.valueOf(properties.getProperty("shardNum"));
        replicaNum = Integer.valueOf(properties.getProperty("replicaNum"));
        principal = properties.getProperty("principal");
        maxShardsPerNode = Integer.valueOf(properties.getProperty("maxShardsPerNode"));
        autoAddReplicas = Boolean.parseBoolean(properties.getProperty("autoAddReplicas"));
        sharedFsReplication =
                properties.getProperty("sharedFsReplication") == null
                        ? null
                        : Boolean.parseBoolean(properties.getProperty("sharedFsReplication"));
        assignToSpecifiedNodeSet = Boolean.parseBoolean(properties.getProperty("assignToSpecifiedNodeSet"));
        createNodeSet = properties.getProperty("createNodeSet");
    }

    private CloudSolrClient getCloudSolrClient(String zkHost) throws SolrException {
        Builder builder = new CloudSolrClient.Builder();
        builder.withZkHost(zkHost);
        CloudSolrClient cloudSolrClient = builder.build();

        cloudSolrClient.setZkClientTimeout(zkClientTimeout);
        cloudSolrClient.setZkConnectTimeout(zkConnectTimeout);
        cloudSolrClient.connect();
        LOG.info("The cloud Server has been connected !!!!");

        ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
        ClusterState cloudState = zkStateReader.getClusterState();
        LOG.info("The zookeeper state is : {}", cloudState);

        return cloudSolrClient;
    }

    private List<Map<String, Object>> queryIndex(CloudSolrClient cloudSolrClient) throws SolrException {
        SolrQuery query = new SolrQuery();
        query.setQuery("name:Luna*");

        try {
            QueryResponse response = cloudSolrClient.query(query);
            SolrDocumentList docs = response.getResults();
            LOG.info("Query wasted time : {}ms", response.getQTime());
            List<Map<String, Object>> resultList = new ArrayList<>();
            LOG.info("Total doc num : {}", docs.getNumFound());
            for (SolrDocument doc : docs) {
                LOG.info("doc detail : " + doc.getFieldValueMap());
                resultList.add(doc.getFieldValueMap());
            }
            return resultList;
        } catch (SolrServerException e) {
            LOG.error("Failed to query document", e);
            throw new SolrException("Failed to query document");
        } catch (IOException e) {
            LOG.error("Failed to query document", e);
            throw new SolrException("Failed to query document");
        } catch (Exception e) {
            LOG.error("Failed to query document", e);
            throw new SolrException("unknown exception");
        }
    }

    private List<String> transferMapToList(Map<String, Object> rawMap) {
        StringBuilder sb = new StringBuilder();
        List<String> tempList = new ArrayList<String>();
        for (String key : rawMap.keySet()) {
            tempList.add(sb.append(key + ":" + rawMap.get(key).toString()).toString());
        }

        return tempList;
    }

    private void removeIndex(CloudSolrClient cloudSolrClient) throws SolrException {
        try {
            cloudSolrClient.deleteByQuery("*:*");
            LOG.info("Success to delete index");
        } catch (SolrServerException e) {
            LOG.error("Failed to remove document", e);
            throw new SolrException("Failed to remove document");
        } catch (IOException e) {
            LOG.error("Failed to remove document", e);
            throw new SolrException("Failed to remove document");
        }
    }

    private void addDocs(CloudSolrClient cloudSolrClient) throws SolrException {
        Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
        for (Integer i = 0; i < 5; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i.toString());
            doc.addField("name", "Luna_" + i);
            doc.addField("features", "test" + i);
            doc.addField("price", (float) i * 1.01);
            documents.add(doc);
        }
        try {
            cloudSolrClient.add(documents);
            LOG.info("success to add index");
        } catch (SolrServerException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (IOException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (Exception e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("unknown exception");
        }
    }

    private void addDocs2(CloudSolrClient cloudSolrClient) throws SolrException {
        UpdateRequest request = new UpdateRequest();
        Collection<SolrInputDocument> documents = new ArrayList<SolrInputDocument>();
        for (Integer i = 5; i < 10; i++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", i.toString());
            doc.addField("name", "Luna_" + i);
            doc.addField("features", "test" + i);
            doc.addField("price", (float) i * 1.01);
            documents.add(doc);
        }
        request.add(documents);
        try {
            cloudSolrClient.request(request);
        } catch (SolrServerException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (IOException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        } catch (Exception e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("unknown exception");
        }
    }

    private void createCollection(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.Create create =
                CollectionAdminRequest.createCollection(collectionName, defaultConfigName, shardNum, replicaNum);
        create.setMaxShardsPerNode(maxShardsPerNode);
        create.setAutoAddReplicas(autoAddReplicas);
        if (assignToSpecifiedNodeSet) {
            create.setCreateNodeSet(createNodeSet);
        }
        CollectionAdminResponse response = null;
        try {
            response = create.process(cloudSolrClient);
        } catch (SolrServerException e) {
            LOG.error("Failed to create collection", e);
            throw new SolrException("Failed to create collection");
        } catch (IOException e) {
            LOG.error("Failed to create collection", e);
            throw new SolrException("Failed to create collection");
        } catch (Exception e) {
            LOG.error("Failed to create collection", e);
            throw new SolrException("unknown exception");
        }
        if (response.isSuccess()) {
            LOG.info("Success to create collection[{}]", collectionName);
        } else {
            LOG.error("Failed to create collection[{}], cause : {}", collectionName, response.getErrorMessages());
            throw new SolrException("Failed to create collection");
        }
    }

    private void deleteCollection(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
        CollectionAdminResponse response = null;
        try {
            response = delete.process(cloudSolrClient);
        } catch (SolrServerException e) {
            LOG.error("Failed to delete collection", e);
            throw new SolrException("Failed to create collection");
        } catch (IOException e) {
            LOG.error("Failed to delete collection", e);
            throw new SolrException("unknown exception");
        } catch (Exception e) {
            LOG.error("Failed to delete collection", e);
            throw new SolrException("unknown exception");
        }
        if (response.isSuccess()) {
            LOG.info("Success to delete collection[{}]", collectionName);
        } else {
            LOG.error("Failed to delete collection[{}], cause : {}", collectionName, response.getErrorMessages());
            throw new SolrException("Failed to delete collection");
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> queryAllCollections(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.List list = new CollectionAdminRequest.List();
        CollectionAdminResponse listRes = null;
        try {
            listRes = list.process(cloudSolrClient);
        } catch (SolrServerException e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("Failed to list collection");
        } catch (IOException e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("Failed to list collection");
        } catch (Exception e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("unknown exception");
        }
        List<String> collectionNames = (List<String>) listRes.getResponse().get("collections");
        LOG.info("All existed collections : {}", collectionNames);
        return collectionNames;
    }
}
