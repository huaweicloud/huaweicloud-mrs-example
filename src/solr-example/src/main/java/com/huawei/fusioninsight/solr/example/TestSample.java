/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.fusioninsight.solr.example;

import com.huawei.solr.client.solrj.util.LoginUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.client.ZKClientConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Solr样例代码
 *
 * @since 2020-03-04
 */
public class TestSample {
    private static final Logger LOG = LogManager.getLogger(TestSample.class);

    private boolean solrKbsEnable;

    private int socketTimeoutMillis;

    private int zkConnectTimeout;

    private String zkHost;

    private boolean zkSslEnable;

    private String zookeeperDefaultServerPrincipal;

    private String collectionName;

    private String defaultConfigName;

    private int shardNum;

    private int replicaNum;

    private String principal;

    private int maxShardsPerNode;

    private boolean autoAddReplicas;

    private boolean assignToSpecifiedNodeSet;

    private String createNodeSet;

    private boolean isNeedZkClientConfig;

    private ZKClientConfig zkClientConfig;

    public static void main(String[] args) throws SolrException {
        TestSample testSample = new TestSample();

        testSample.initProperties();
        if (testSample.solrKbsEnable) {
            testSample.setSecConfig();
        }

        if (testSample.zkSslEnable) {
            LoginUtil.setZKSSLParameters();
        }

        CloudSolrClient cloudSolrClient = null;
        try {
            cloudSolrClient = testSample.getCloudSolrClient(testSample.zkHost);

            List<String> collectionNames = testSample.queryAllCollections(cloudSolrClient);

            if (collectionNames.contains(testSample.collectionName)) {
                testSample.deleteCollection(cloudSolrClient);
            }

            testSample.createCollection(cloudSolrClient);

            cloudSolrClient.setDefaultCollection(testSample.collectionName);

            testSample.addDocs(cloudSolrClient);

            testSample.addDocs2(cloudSolrClient);

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOG.warn("Thread sleep exception.", e);
            }

            testSample.queryIndex(cloudSolrClient);

            testSample.removeIndex(cloudSolrClient);

            testSample.queryIndex(cloudSolrClient);

        } catch (SolrException solrException) {
            throw new SolrException(solrException.getMessage());
        } finally {
            if (cloudSolrClient != null) {
                try {
                    cloudSolrClient.close();
                } catch (IOException e) {
                    LOG.warn("Failed to close cloudSolrClient.", e);
                }
            }
        }
    }

    private void initProperties() throws SolrException {
        Properties properties = new Properties();
        String proPath =
                System.getProperty("user.dir") + File.separator + "conf" + File.separator + "solr-example.properties";
        try {
            properties.load(new FileInputStream(new File(proPath)));
        } catch (IOException e) {
            throw new SolrException("Failed to load properties file.");
        }
        solrKbsEnable = Boolean.parseBoolean(properties.getProperty("SOLR_KBS_ENABLED"));
        socketTimeoutMillis = Integer.parseInt(properties.getProperty("socketTimeoutMillis"));
        zkConnectTimeout = Integer.parseInt(properties.getProperty("zkConnectTimeout"));
        zkHost = properties.getProperty("zkHost");
        zkSslEnable = Boolean.parseBoolean(properties.getProperty("zkSslEnable"));
        zookeeperDefaultServerPrincipal = properties.getProperty("ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL");
        collectionName = properties.getProperty("COLLECTION_NAME");
        defaultConfigName = properties.getProperty("DEFAULT_CONFIG_NAME");
        shardNum = Integer.parseInt(properties.getProperty("shardNum"));
        replicaNum = Integer.parseInt(properties.getProperty("replicaNum"));
        principal = properties.getProperty("principal");
        maxShardsPerNode = Integer.parseInt(properties.getProperty("maxShardsPerNode"));
        autoAddReplicas = Boolean.parseBoolean(properties.getProperty("autoAddReplicas"));
        assignToSpecifiedNodeSet = Boolean.parseBoolean(properties.getProperty("assignToSpecifiedNodeSet"));
        createNodeSet = properties.getProperty("createNodeSet");
        isNeedZkClientConfig = Boolean.parseBoolean(properties.getProperty("isNeedZkClientConfig"));
    }

    private void setSecConfig() throws SolrException {
        String path = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        path = path.replace("\\", "\\\\");

        // The following statement imply that you can use SolrClient section in jaas.conf
        try {
            LoginUtil.setJaasFile(principal, path + "user.keytab");
            LoginUtil.setKrb5Config(path + "krb5.conf");
            LoginUtil.setZookeeperServerPrincipal(zookeeperDefaultServerPrincipal);
            if (this.isNeedZkClientConfig) {
                this.setZkClientConfig();
            }
        } catch (IOException e) {
            LOG.error("Failed to set security conf", e);
            throw new SolrException("Failed to set security conf");
        }
    }

    private void setZkClientConfig() {
        zkClientConfig = new ZKClientConfig();
        zkClientConfig.setProperty(ZKClientConfig.LOGIN_CONTEXT_NAME_KEY, "SolrClient");
        zkClientConfig.setProperty(ZKClientConfig.ENABLE_CLIENT_SASL_KEY, "true");
        zkClientConfig.setProperty(ZKClientConfig.ZOOKEEPER_SERVER_PRINCIPAL, "zookeeper/HADOOP.HADOOP.COM");
    }

    private CloudSolrClient getCloudSolrClient(String zkHost) throws SolrException {
        CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
                .withConnectionTimeout(zkConnectTimeout)
                .withSocketTimeout(socketTimeoutMillis)
                .build();

        cloudSolrClient.setisNeedZkClientConfig(isNeedZkClientConfig);
        if (isNeedZkClientConfig) {
            cloudSolrClient.setZkClientConfig(zkClientConfig);
        }
        cloudSolrClient.connect();
        LOG.info("The cloud Server has been connected !!!!");

        ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
        ClusterState cloudState = zkStateReader.getClusterState();
        LOG.info("The zookeeper state is : {}", cloudState);

        return cloudSolrClient;
    }

    private void queryIndex(CloudSolrClient cloudSolrClient) throws SolrException {
        SolrQuery query = new SolrQuery();
        query.setQuery("name:Luna*");

        try {
            QueryResponse response = cloudSolrClient.query(query);
            SolrDocumentList docs = response.getResults();
            LOG.info("Query wasted time : {}ms", response.getQTime());
            LOG.info("Total doc num : {}", docs.getNumFound());
            for (SolrDocument doc : docs) {
                LOG.info("doc detail : {}", doc.getFieldValueMap().toString());
            }
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to query document", e);
            throw new SolrException("Failed to query document");
        }
    }

    private void removeIndex(CloudSolrClient cloudSolrClient) throws SolrException {
        try {
            cloudSolrClient.deleteByQuery("*:*");
            LOG.info("Success to delete index");
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to remove document", e);
            throw new SolrException("Failed to remove document");
        }
    }

    private void addDocs(CloudSolrClient cloudSolrClient) throws SolrException {
        Collection<SolrInputDocument> documents = getSolrInputDocuments();
        try {
            cloudSolrClient.add(documents);
            LOG.info("success to add index");
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        }
    }

    private void addDocs2(CloudSolrClient cloudSolrClient) throws SolrException {
        UpdateRequest request = new UpdateRequest();
        Collection<SolrInputDocument> documents = getSolrInputDocuments();
        request.add(documents);
        try {
            cloudSolrClient.request(request);
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to add document to collection", e);
            throw new SolrException("Failed to add document to collection");
        }
    }

    private Collection<SolrInputDocument> getSolrInputDocuments() {
        Collection<SolrInputDocument> documents = new ArrayList<>();
        int documentCount = 5;
        for (int id = 0; id < documentCount; id++) {
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", id);
            doc.addField("name", "Luna_" + id);
            doc.addField("features", "test" + id);
            doc.addField("price", id * 2);
            documents.add(doc);
        }
        return documents;
    }

    private void createCollection(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.Create create =
            CollectionAdminRequest.createCollection(collectionName, defaultConfigName, shardNum, replicaNum);
        create.setMaxShardsPerNode(maxShardsPerNode);
        create.setAutoAddReplicas(autoAddReplicas);
        if (assignToSpecifiedNodeSet) {
            create.setCreateNodeSet(createNodeSet);
        }
        CollectionAdminResponse response;
        try {
            response = create.process(cloudSolrClient);
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to create collection.", e);
            throw new SolrException("Failed to create collection.");
        }
        if (response.isSuccess()) {
            LOG.info("Success to create collection[{}].", collectionName);
        } else {
            LOG.error("Failed to create collection[{}], cause : {}.", collectionName, response.getErrorMessages());
            throw new SolrException("Failed to create collection.");
        }
    }

    private void deleteCollection(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(collectionName);
        CollectionAdminResponse response;
        try {
            response = delete.process(cloudSolrClient);
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to delete collection.", e);
            throw new SolrException("Failed to create collection.");
        }
        if (response.isSuccess()) {
            LOG.info("Success to delete collection[{}].", collectionName);
        } else {
            LOG.error("Failed to delete collection[{}], cause : {}.", collectionName, response.getErrorMessages());
            throw new SolrException("Failed to delete collection.");
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> queryAllCollections(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.List list = new CollectionAdminRequest.List();
        CollectionAdminResponse listRes;
        try {
            listRes = list.process(cloudSolrClient);
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("Failed to list collection");
        }
        List<String> collectionNames = new ArrayList<>();
        if (listRes.getResponse().get("collections") instanceof List) {
            collectionNames = (List<String>) listRes.getResponse().get("collections");
        }
        LOG.info("All existed collections : {}", collectionNames);
        return collectionNames;
    }
}
