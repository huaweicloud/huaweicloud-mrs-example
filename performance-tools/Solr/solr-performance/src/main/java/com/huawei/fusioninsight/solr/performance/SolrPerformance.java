/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2021. All rights reserved.
 */

package com.huawei.fusioninsight.solr.performance;

import com.huawei.solr.client.solrj.util.LoginUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkStateReader;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;

public class SolrPerformance {
    private static final Logger LOG = LogManager.getLogger(SolrPerformance.class);

    private static final String CONFDIR = "conf";

    private static final String OP_CREATE = "create";

    private static final String OP_PUT = "put";

    private static final String OP_QUERY = "query";

    private String solrKbsEnable;

    private int socketTimeoutMillis;

    private int zkConnectTimeout;

    private String zkHost;

    private boolean zkSslEnable;

    private String zookeeperDefaultServerPrincipal;

    private String collectionName;

    private String createCollectionName;

    private String defaultConfigName;

    private int shardNum;

    private int replicaNum;

    private String principal;

    private static int inputLoops;

    private static int getLoops;

    private static int inputRows;

    private static int threadsNumPut;

    private static int threadsNumGet;

    private int maxShardsPerNode;

    private String exactQueryStatement;

    // 重试次数
    private static final int TRY_TIMES = 30;

    // 重试间隔时间单位毫秒
    private static final int INTERVAL_TIME = 10000;

    /**
     * Solr性能测试工具main方法
     *
     * @param args 参数输入
     * @throws SolrException Solr自定义异常
     * @throws InterruptedException 异常
     * @throws ExecutionException 异常
     */
    public static void main(String[] args) throws SolrException, InterruptedException, ExecutionException {
        SolrPerformance solrPerformance = new SolrPerformance();
        CloudSolrClient cloudSolrClient = null;

        try {
            solrPerformance.initProperties();
            solrPerformance.initArgs(args);

            cloudSolrClient = solrPerformance.getCloudSolrClient(solrPerformance.zkHost);
            cloudSolrClient.setDefaultCollection(solrPerformance.collectionName);

            long startTime = System.currentTimeMillis();
            if (OP_CREATE.equals(args[1])) {
                solrPerformance.create(cloudSolrClient);
            } else if (OP_PUT.equals(args[1])) {
                solrPerformance.put(cloudSolrClient, args[2]);
            } else if (OP_QUERY.equals(args[1])) {
                solrPerformance.query(cloudSolrClient, args[2]);
            } else {
                LOG.warn("Input the correct operation: create,put,query.");
            }
            LOG.info("Finished Task.");
            LOG.info("Program running time is ：{}.", (System.currentTimeMillis() - startTime));
        } finally {
            cloudSolrClient(cloudSolrClient);
        }
    }

    private void query(CloudSolrClient cloudSolrClient, String proId) throws ExecutionException, InterruptedException {
        List<Future<?>> futures = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threadsNumGet);
        for (int i = 1; i <= threadsNumGet; ++i) {
            final int k = Integer.parseInt(proId);
            Future<?> future = executorService.submit(() -> {
                try {
                    queryIndex(k, cloudSolrClient);
                } catch (SolrException solrException) {
                    LOG.error("Query operation failed.", solrException);
                }
            });
            futures.add(future);
        }
        for (Future<?> future : futures) {
            future.get();
        }
        executorService.shutdownNow();
    }

    private void put(CloudSolrClient cloudSolrClient, String prid) throws ExecutionException, InterruptedException {
        List<Future<?>> futures = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(threadsNumPut);
        for (int i = 0; i < threadsNumPut; i++) {
            final int j = i;
            final String k = prid;
            Future<?> future = executorService.submit(() -> {
                try {
                    addDocs(j, k, cloudSolrClient);
                } catch (SolrException | InterruptedException e) {
                    LOG.error("Put operation failed.", e);
                }
            });
            futures.add(future);
        }
        for (Future<?> future : futures) {
            future.get();
        }
        executorService.shutdownNow();
    }

    private void create(CloudSolrClient cloudSolrClient) throws SolrException {
        List<String> collectionNames = queryAllCollections(cloudSolrClient);
        if (collectionNames.contains(collectionName)) {
            deleteCollection(cloudSolrClient);
        }
        createCollection(cloudSolrClient);
    }

    private static void cloudSolrClient(CloudSolrClient cloudSolrClient) {
        if (cloudSolrClient != null) {
            try {
                cloudSolrClient.close();
            } catch (IOException e) {
                LOG.warn("Failed to close cloudSolrClient", e);
            }
        }
    }


    private void initArgs(String[] args) throws SolrException {
        if ("mode1".equals(args[0])) {
            collectionName = "Collection_" + args[2];
        }
        if ("mode2".equals(args[0])) {
            collectionName = createCollectionName;
        }
        if ("true".equals(solrKbsEnable)) {
            setSecConfig();
        }
        if (zkSslEnable) {
            LoginUtil.setZKSSLParameters();
        }
        if (args.length >= 4 && !StringUtils.isEmpty(args[3])) {
            this.exactQueryStatement = args[3];
        }
    }

    private void initProperties() throws SolrException {
        Properties properties = new Properties();
        String proPath =
                System.getProperty("user.dir") + File.separator + CONFDIR + File.separator + "solr-performance.properties";
        try {
            properties.load(Files.newInputStream(Paths.get(proPath)));
        } catch (IOException e) {
            throw new SolrException("Failed to load properties file.");
        }
        this.solrKbsEnable = properties.getProperty("SOLR_KBS_ENABLED");
        this.socketTimeoutMillis = Integer.parseInt(properties.getProperty("socketTimeoutMillis"));
        this.zkConnectTimeout = Integer.parseInt(properties.getProperty("zkConnectTimeout"));
        this.zkHost = properties.getProperty("zkHost");
        this.zkSslEnable = Boolean.parseBoolean(properties.getProperty("zkSslEnable"));
        this.zookeeperDefaultServerPrincipal = properties.getProperty("ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL");
        this.createCollectionName = properties.getProperty("COLLECTION_NAME");
        this.defaultConfigName = properties.getProperty("DEFAULT_CONFIG_NAME");
        this.shardNum = Integer.parseInt(properties.getProperty("shardNum"));
        this.replicaNum = Integer.parseInt(properties.getProperty("replicaNum"));
        this.principal = properties.getProperty("principal");
        inputLoops = Integer.parseInt(properties.getProperty("inputLoops"));
        getLoops = Integer.parseInt(properties.getProperty("getLoops"));
        inputRows = Integer.parseInt(properties.getProperty("inputRows"));
        threadsNumPut = Integer.parseInt(properties.getProperty("threadsNumPut"));
        threadsNumGet = Integer.parseInt(properties.getProperty("threadsNumGet"));
        this.maxShardsPerNode = Integer.parseInt(properties.getProperty("maxShardsPerNode"));
    }

    private void setSecConfig() throws SolrException {
        String path = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        path = path.replace("\\", "\\\\");
        try {
            LoginUtil.setJaasFile(this.principal, path + "user.keytab");
            LoginUtil.setKrb5Config(path + "krb5.conf");
            LoginUtil.setZookeeperServerPrincipal(this.zookeeperDefaultServerPrincipal);
        } catch (IOException e) {
            LOG.error("Failed to set security conf", e);
            throw new SolrException("Failed to set security conf");
        }
    }

    private CloudSolrClient getCloudSolrClient(String zkHost) {
        CloudSolrClient cloudSolrClient = new CloudSolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
                .withConnectionTimeout(zkConnectTimeout)
                .withSocketTimeout(socketTimeoutMillis)
                .build();
        cloudSolrClient.connect();
        LOG.info("The cloud Server has been connected !");

        ZkStateReader zkStateReader = cloudSolrClient.getZkStateReader();
        ClusterState cloudState = zkStateReader.getClusterState();
        LOG.info("The zookeeper state is : {}", cloudState);

        return cloudSolrClient;
    }

    private static String getRandomJianHan(int len) {
        StringBuilder ret = new StringBuilder();
        for (int i = 0; i < len; i++) {
            String str = "";
            Random random = new Random();
            int highPos = 176 + Math.abs(random.nextInt(39));
            int lowPos = 161 + Math.abs(random.nextInt(93));
            byte[] b = new byte[2];
            b[0] = (Integer.valueOf(highPos)).byteValue();
            b[1] = (Integer.valueOf(lowPos)).byteValue();
            try {
                str = new String(b, "GBK");
            } catch (UnsupportedEncodingException ex) {
                LOG.error(ex.getMessage());
            }
            ret.append(str);
        }
        return ret.toString();
    }

    private void addDocs(int thId, String prId, CloudSolrClient cloudSolrClient) throws SolrException, InterruptedException {
        for (int j = 0; j < inputLoops; j++) {
            Collection<SolrInputDocument> documents = new ArrayList<>();
            for (int i = 0; i < inputRows; i++) {
                SolrInputDocument doc = new SolrInputDocument();
                doc.addField("id", thId + "_" + prId + "_" + j + "_" + i);
                doc.addField("name", getRandomJianHan(3));
                doc.addField("features", getRandomJianHan(10));
                doc.addField("title", getRandomJianHan(20));
                doc.addField("description", getRandomJianHan(20));
                doc.addField("comments", getRandomJianHan(ThreadLocalRandom.current().nextInt(150, 250)));
                doc.addField("keywords", getRandomJianHan(ThreadLocalRandom.current().nextInt(1, 40)));
                doc.addField("price", ThreadLocalRandom.current().nextInt(18, 100));
                doc.addField("weight", ThreadLocalRandom.current().nextInt(0, 2));
                doc.addField("popularity", ThreadLocalRandom.current().nextInt(140, 220));
                doc.addField("subject", getRandomJianHan(2));
                doc.addField("author", getRandomJianHan(2));
                doc.addField("category", getRandomJianHan(2));
                doc.addField("last_modified", Calendar.getInstance().getTime());
                documents.add(doc);
            }
            add(cloudSolrClient, documents);
        }
    }

    private void add(CloudSolrClient cloudSolrClient, Collection<SolrInputDocument> documents) throws InterruptedException, SolrException {
        int retryNum = 1;
        Throwable throwable = null;
        while (retryNum <= TRY_TIMES) {
            try {
                cloudSolrClient.add(documents);
                LOG.info("Success to add index.");
                return;
            } catch (Exception e) {
                retryNum++;
                LOG.warn("Retry {} times after {}ms of sleep.", retryNum, INTERVAL_TIME);
                throwable = e;
                Thread.sleep(INTERVAL_TIME);
            }
        }
        throw new SolrException("Failed to add document to collection.", throwable);
    }

    private void createCollection(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(this.collectionName,
                this.defaultConfigName, this.shardNum, this.replicaNum);

        create.setMaxShardsPerNode(this.maxShardsPerNode);

        CollectionAdminResponse response;
        try {
            response = create.process(cloudSolrClient);
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to create collection", e);
            throw new SolrException("Failed to create collection");
        } catch (Exception e) {
            LOG.error("Failed to create collection", e);
            throw new SolrException("unknown exception");
        }
        if (response.isSuccess()) {
            LOG.info("Success to create collection[{}]", this.collectionName);
        } else {
            LOG.error("Failed to create collection[{}], cause : {}", this.collectionName, response.getErrorMessages());
            throw new SolrException("Failed to create collection");
        }
    }

    private void deleteCollection(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.Delete delete = CollectionAdminRequest.deleteCollection(this.collectionName);
        CollectionAdminResponse response;
        try {
            response = delete.process(cloudSolrClient);
        } catch (SolrServerException e) {
            LOG.error("Failed to delete collection", e);
            throw new SolrException("Failed to create collection");
        } catch (Exception e) {
            LOG.error("Failed to delete collection", e);
            throw new SolrException("unknown exception");
        }
        if (response.isSuccess()) {
            LOG.info("Success to delete collection[{}]", this.collectionName);
        } else {
            LOG.error("Failed to delete collection[{}], cause : {}", this.collectionName, response.getErrorMessages());
            throw new SolrException("Failed to delete collection");
        }
    }

    private List<String> queryAllCollections(CloudSolrClient cloudSolrClient) throws SolrException {
        CollectionAdminRequest.List list = new CollectionAdminRequest.List();
        CollectionAdminResponse listRes;
        try {
            listRes = list.process(cloudSolrClient);
        } catch (SolrServerException | IOException e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("Failed to list collection");
        } catch (Exception e) {
            LOG.error("Failed to list collection", e);
            throw new SolrException("unknown exception");
        }
        List<String> collectionNames = (List) listRes.getResponse().get("collections");
        LOG.info("All existed collections : {}", collectionNames);
        return collectionNames;
    }

    private void queryIndex(int proId, CloudSolrClient cloudSolrClient) throws SolrException {
        if (StringUtils.isEmpty(exactQueryStatement)) {
            queryIndexWithoutId(proId, cloudSolrClient);
        } else {
            SolrQuery query = new SolrQuery();
            int exactQueryTimes = 0;
            int exactQueryDocs = 0;
            String queryCondition = "id:" + exactQueryStatement;
            query.setQuery(queryCondition);
            for (int i = 0; i < getLoops; i++) {
                try {
                    QueryResponse response = cloudSolrClient.query(query);
                    exactQueryTimes += response.getQTime();
                    exactQueryDocs += response.getResults().getNumFound();
                } catch (SolrServerException | IOException e) {
                    LOG.error("Failed to query document", e);
                    throw new SolrException("Failed to query document");
                } catch (Exception e) {
                    LOG.error("Failed to query document", e);
                    throw new SolrException("unknown exception");
                }
            }
            LOG.info("Current collection is {}.", cloudSolrClient.getDefaultCollection());
            LOG.info("Query condition is {}, The number of query is {}, The total number of documents is {}, Query wasted time {}ms, Time spent per query is {}ms.",
                    queryCondition, getLoops, exactQueryDocs, exactQueryTimes, exactQueryTimes / getLoops);
        }
    }

    static class QueryResult {

        private int resultTime = 0;

        private long num = 0;


        public int getResultTime() {
            return resultTime;
        }

        public void setResultTime(int resultTime) {
            this.resultTime = resultTime;
        }

        public long getNum() {
            return num;
        }

        public void setNum(long num) {
            this.num = num;
        }
    }

    private void queryIndexWithoutId(int proId, CloudSolrClient cloudSolrClient) throws SolrException {
        SolrQuery query = new SolrQuery();
        List<String> queryArray = null;
        QueryResult queryResult1 = new QueryResult();
        QueryResult queryResult2 = new QueryResult();
        QueryResult queryResult3 = new QueryResult();
        QueryResult queryResult4 = new QueryResult();

        for (int i = 0; i < getLoops; i++) {
            queryArray = new ArrayList<>(getQueryCondition(proId));
            for (int j = 1; j <= 4; j++) {
                try {
                    query.setQuery(queryArray.get(j - 1));
                    QueryResponse response = cloudSolrClient.query(query);
                    if (j == 1) {
                        setResult(queryResult1, response);
                    }
                    if (j == 2) {
                        setResult(queryResult2, response);
                    }
                    if (j == 3) {
                        setResult(queryResult3, response);
                    }
                    if (j == 4) {
                        setResult(queryResult4, response);
                    }
                } catch (SolrServerException | IOException e) {
                    throw new SolrException("Failed to query document", e);
                } catch (Exception e) {
                    throw new SolrException("unknown exception", e);
                }
            }
        }
        String logContent = "Current collection is {}, Query condition is {}, The number of query is {}, The total number of documents is {}, Query wasted time {}ms, Time spent per query is {}ms.";
        assert queryArray != null;
        LOG.info(logContent, cloudSolrClient.getDefaultCollection(), queryArray.get(0), getLoops, queryResult1.getNum(), queryResult1.getResultTime(), queryResult1.getResultTime() / getLoops);
        LOG.info(logContent, cloudSolrClient.getDefaultCollection(), queryArray.get(1), getLoops, queryResult2.getNum(), queryResult2.getResultTime(), queryResult2.getResultTime() / getLoops);
        LOG.info(logContent, cloudSolrClient.getDefaultCollection(), queryArray.get(2), getLoops, queryResult3.getNum(), queryResult3.getResultTime(), queryResult3.getResultTime() / getLoops);
        LOG.info(logContent, cloudSolrClient.getDefaultCollection(), queryArray.get(3), getLoops, queryResult4.getNum(), queryResult4.getResultTime(), queryResult4.getResultTime() / getLoops);
    }

    private void setResult(QueryResult queryResult, QueryResponse response) {
        SolrDocumentList docs = response.getResults();
        queryResult.setResultTime(queryResult.getResultTime() + response.getQTime());
        queryResult.setNum(queryResult.getNum() + docs.getNumFound());
    }

    private List<String> getQueryCondition(int proId) {
        Random rnd = new Random();
        int rndThread = rnd.nextInt(threadsNumPut);
        int rndLoop = rnd.nextInt(inputLoops);
        int rndRow = rnd.nextInt(inputRows);

        String queryCondition1 = "id:*_*_" + rndLoop + "_" + rndRow;
        String queryCondition2 = "id:*_*_*_" + rndRow;
        String queryCondition3 = "id:" + rndThread + "_*_*_" + rndRow;
        String queryCondition4 = "id:" + rndThread + "_" + proId + "_" + rndLoop + "_*";

        return Arrays.asList(queryCondition1, queryCondition2, queryCondition3, queryCondition4);
    }
}
