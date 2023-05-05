/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */
package com.huawei.fusioninsight.es.performance;

import net.sf.json.JSONObject;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * ES性能工具
 *
 * @since 2022-09-01
 */
public class ESPerformanceMain {
    private static long threadCommitNum;
    private static final Logger LOG = LoggerFactory.getLogger(ESPerformanceMain.class);
    private static String schema = "https";
    private static RestClient restClient = null;
    private static ConfigInfo configInfo;

    /**
     * Get random Chinese string
     */
    public static String getRandomChinese(int len) {
        StringBuffer ret = new StringBuffer();
        Random random = new Random();
        for (int i = 0; i < len; i++) {
            String str = "";
            int highPos = 176 + Math.abs(random.nextInt(39));
            int lowPos = 161 + Math.abs(random.nextInt(93));
            byte[] byteArray = new byte[2];
            byteArray[0] = (new Integer(highPos)).byteValue();
            byteArray[1] = (new Integer(lowPos)).byteValue();
            try {
                str = new String(byteArray, "GBK");
            } catch (UnsupportedEncodingException ex) {
                LOG.error("Encoding failed.", ex);
            }
            ret.append(str);
        }
        return ret.toString();
    }

    private static void setSecConfig() {
        String krb5ConfFile = String.valueOf(System.getProperty("user.dir")) + File.separator + "conf" + File.separator + "krb5.conf";
        System.setProperty("java.security.krb5.conf", krb5ConfFile);

        String jaasPath = String.valueOf(System.getProperty("user.dir")) + File.separator + "conf" + File.separator + "jaas.conf";
        System.setProperty("java.security.auth.login.config", jaasPath);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
    }

    /**
     * Get host addresses
     */
    public static HttpHost[] getHostArray(String esServerHost) throws Exception {
        if (configInfo.isSecurity == 0) {
            schema = "http";
        }
        List<HttpHost> hosts = new ArrayList<HttpHost>();
        String[] hostArray1 = esServerHost.split(",");

        for (String host : hostArray1) {
            String[] ipPort = host.split(":");
            HttpHost hostNew = new HttpHost(ipPort[0], Integer.parseInt(ipPort[1]), schema);
            hosts.add(hostNew);
        }
        return hosts.toArray(new HttpHost[0]);
    }

    private static RestClient getRestClient(HttpHost[] HostArray) throws Exception {
        RestClientBuilder builder = null;
        if (configInfo.isSecurity == 1) {
            setSecConfig();
            System.setProperty("es.security.indication", "true");
            builder = RestClient.builder(HostArray);
        } else {
            System.setProperty("es.security.indication", "false");
            builder = RestClient.builder(HostArray);
        }
        Header[] defaultHeaders = {new BasicHeader("Accept", "application/json"),
                new BasicHeader("Content-type", "application/json")};

        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setConnectTimeout(configInfo.connectTimeout)
                        .setSocketTimeout(configInfo.socketTimeout);
            }
        });
        builder.setDefaultHeaders(defaultHeaders);
        restClient = builder.build();
        LOG.info("The RestClient has been created !");
        return restClient;
    }

    private static Map<String, Object> esJsonInput(String strId, int strLength) {
        Map<String, Object> esJson = new HashMap<String, Object>();
        esJson.put("id", strId);
        esJson.put("sex", ThreadLocalRandom.current().nextInt(0, 2));
        esJson.put("age", ThreadLocalRandom.current().nextInt(18, 100));
        esJson.put("height", (float) ThreadLocalRandom.current().nextInt(140, 220));
        esJson.put("weight", (float) ThreadLocalRandom.current().nextInt(70, 200));
        esJson.put("cur_time", (new Date()).getTime());
        esJson.put("income", ThreadLocalRandom.current().nextInt(2, 100000000));
        esJson.put("marriage", ThreadLocalRandom.current().nextInt(0, 2));
        esJson.put("name", getRandomChinese(strLength));
        esJson.put("education", getRandomChinese(strLength));
        esJson.put("place", getRandomChinese(strLength));
        esJson.put("hobby", getRandomChinese(strLength));
        esJson.put("introduce", getRandomChinese(strLength));
        esJson.put("others", getRandomChinese(strLength));
        return esJson;
    }

    public static void dataInput(long recordNum, String threadinfo) throws Exception {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        String processPid = processName.split("@")[0];
        Map<String, Object> esJson = new HashMap<String, Object>();
        long idSeqNumber = 0L;
        long circleCommit = recordNum / 1000L;
        JSONObject json = null;
        String str = "{ \"index\" : { \"_index\" : \"" + configInfo.index + "\"} }";
        for (int j = 0; j < circleCommit; j++) {
            Date time = new Date();
            StringBuffer buffer = new StringBuffer();
            for (int i = 0; i < 1000; i++) {
                esJson.clear();
                idSeqNumber = idSeqNumber + 1L;
                idSeqNumber = idSeqNumber + 1L;
                String strId = processPid + "_" + threadinfo + "_" + idSeqNumber;
                int strLength = configInfo.singleRecordData == 1000 ? 72 : 415;
                esJson = esJsonInput(strId, strLength);
                json = JSONObject.fromObject(esJson);
                buffer.append(str).append("\n");
                buffer.append(json).append("\n");
            }
            StringEntity entity = new StringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
            entity.setContentEncoding("UTF-8");

            Response response = null;
            try {
                Request request = new Request("PUT", "/_bulk");
                request.setEntity(entity);
                request.addParameter("pretty", "true");
                response = restClient.performRequest(request);

                if (response.getStatusLine().getStatusCode() != 200 &&
                        response.getStatusLine().getStatusCode() != 201) {
                    LOG.info(time + "==putData failed." + EntityUtils.toString(response.getEntity()));
                    LOG.error(time + "==putData failed.");
                }
            } catch (Exception e) {
                LOG.error(time + "==putData failed." + e);
            }
        }
    }

    private static void queryAtFixedRate(long recordNum) {
        long circleCommit = recordNum / 1000L;
        for (int i = 0; i < circleCommit; i++) {
            for (int j = 0; j < 1000; j++) {
                int age = ThreadLocalRandom.current().nextInt(18, 100);

                BufferedReader rd = null;
                String line = "";
                String time = "";
                String records = "";
                try {
                    Request request = new Request("GET", "/" + configInfo.index + "/" + configInfo.type + "/_search?size=1&q=age:" + age);
                    request.addParameter("pretty", "true");
                    Response response = restClient.performRequest(request);
                    if (response.getStatusLine().getStatusCode() == 200L) {
                        rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                        StringBuffer result = new StringBuffer();
                        while ((line = rd.readLine()) != null) {
                            result.append(line);
                        }
                        response.getEntity().getContent().close();
                        time = result.substring(result.indexOf("took") + 6, result.indexOf("timed_out") - 2);
                        String tmp = result.substring(result.indexOf("hits"));
                        records = tmp.substring(tmp.indexOf("total") + 7, tmp.indexOf("max_score") - 2);

                        LOG.info("queryString: " + age + " ,queryTime: " + time + " ,hitRecords: " + records);
                    }
                } catch (IOException e) {
                    LOG.error("Query failed.");
                }
            }
        }
    }

    private static void queryByCondition(long recordNum, char typeCondition) {
        long circleCommit = recordNum / 1000L;
        for (int i = 0; i < circleCommit; i++) {
            for (int j = 0; j < 1000; j++) {
                int age = ThreadLocalRandom.current().nextInt(18, 100);
                String introduce = getRandomChinese(1);
                String place = getRandomChinese(1);
                String hobby = getRandomChinese(1);

                String str;
                if (typeCondition == '1') {
                    str = "{\"query\":{\"match\":{\"introduce\":\"" + introduce + "\"" + "}}}";
                } else {
                    str = "{\"query\":{\"bool\":{\"should\":[{\"match\":{\"introduce\":\"" +
                            introduce + "\"}}," +
                            "{" + "\"match\":{" + "\"place\":\"" + place + "\"}}," +
                            "{" + "\"match\":{" + "\"hobby\":\"" + hobby + "\"}}" +
                            "]" + "}}}";
                }
                StringEntity entity = new StringEntity(str, ContentType.APPLICATION_JSON);
                entity.setContentEncoding("UTF-8");
                Map<String, String> params = Collections.singletonMap("pretty", "true");

                BufferedReader rd = null;
                String line = "";
                String time = "";
                String records = "";

                try {
                    Request request = new Request("GET", "/_search");
                    request.setEntity(entity);
                    request.addParameter("pretty", "true");
                    Response response = restClient.performRequest(request);
                    if (response.getStatusLine().getStatusCode() == 200L) {
                        rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                        StringBuffer result = new StringBuffer();
                        while ((line = rd.readLine()) != null) {
                            result.append(line);
                        }
                        response.getEntity().getContent().close();
                        time = result.substring(result.indexOf("took") + 6, result.indexOf("timed_out") - 2);
                        String tmp = result.substring(result.indexOf("hits"));
                        records = tmp.substring(tmp.indexOf("total") + 7, tmp.indexOf("max_score") - 2);
                        if (typeCondition == '1') {
                            LOG.info("queryString: " + introduce + " ,queryTime: " + time + " ,hitRecords: " + records);
                        } else {
                            LOG.info("queryString: " + introduce + place + hobby + " ,queryTime: " + time + " ,hitRecords: " + records);
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Query failed.");
                }
            }
        }
    }

    private static void queryByBlurred(long recordNum, char typeBlurred) {
        long circleCommit = recordNum / 1000L;
        for (int i = 0; i < circleCommit; i++) {
            for (int j = 0; j < 1000; j++) {
                int age = ThreadLocalRandom.current().nextInt(18, 100);
                String introduce = getRandomChinese(1);
                String place = getRandomChinese(1);
                String hobby = getRandomChinese(1);
                String str;
                if (typeBlurred == '1') {
                    str = "{\"query\":{\"wildcard\":{\"id\":\"grwwY2sB3JmWL7ohKdC*\"}}}";
                } else if (typeBlurred == '3') {
                    str = "{\"query\":{\"wildcard\":{\"id\":\"grwwY2sB3JmWL7ohK*\"}}}";
                } else {
                    str = "{\"query\":{\"wildcard\":{\"id\":\"grwwY2sB3JmWL7o*\"}}}";
                }
                StringEntity entity = new StringEntity(str, ContentType.APPLICATION_JSON);
                entity.setContentEncoding("UTF-8");
                Map<String, String> params = Collections.singletonMap("pretty", "true");

                BufferedReader rd = null;
                String line = "";
                String time = "";
                String records = "";
                try {
                    Request request = new Request("GET", "/_search");
                    request.setEntity(entity);
                    request.addParameter("pretty", "true");
                    Response response = restClient.performRequest(request);
                    if (response.getStatusLine().getStatusCode() == 200L) {
                        rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                        StringBuffer result = new StringBuffer();
                        while ((line = rd.readLine()) != null) {
                            result.append(line);
                        }
                        response.getEntity().getContent().close();
                        time = result.substring(result.indexOf("took") + 6, result.indexOf("timed_out") - 2);
                        String tmp = result.substring(result.indexOf("hits"));
                        records = tmp.substring(tmp.indexOf("total") + 7, tmp.indexOf("max_score") - 2);

                        if (typeBlurred == '1') {
                            LOG.info("queryString: grwwY2sB3JmWL7ohKdC* ,queryTime: " + time + " ,hitRecords: " + records);
                        } else if (typeBlurred == '3') {
                            LOG.info("queryString: grwwY2sB3JmWL7ohK* ,queryTime: " + time + " ,hitRecords: " + records);
                        } else {
                            LOG.info("queryString: grwwY2sB3JmWL7o* ,queryTime: " + time + " ,hitRecords: " + records);
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Query failed.");
                }
            }
        }
    }

    private static void queryByField(long recordNum, char typeField) {
        long circleCommit = recordNum / 1000L;
        for (int i = 0; i < circleCommit; i++) {
            for (int j = 0; j < 1000; j++) {
                int age = ThreadLocalRandom.current().nextInt(18, 100);
                String introduce = getRandomChinese(1);
                String place = getRandomChinese(1);
                String hobby = getRandomChinese(1);

                String str = "{\"query\":{\"bool\":{\"should\":[{\"match\":{\"introduce\":\"" +
                        introduce + "\"}}," +
                        "{" + "\"match\":{" + "\"place\":\"" + place + "\"}}," +
                        "{" + "\"match\":{" + "\"hobby\":\"" + hobby + "\"}}" +
                        "]" + "}}}";
                StringEntity entity = new StringEntity(str, ContentType.APPLICATION_JSON);
                entity.setContentEncoding("UTF-8");
                Map<String, String> params = Collections.singletonMap("pretty", "true");

                BufferedReader rd = null;
                String line = "";
                String time = "";
                String records = "";
                try {
                    Response response;
                    Request request;
                    if (typeField == '1') {
                        request = new Request("GET", "/_search");
                    } else {
                        request = new Request("GET", "/" + configInfo.index + "/" + configInfo.type + "/_search");
                    }
                    request.setEntity(entity);
                    request.addParameter("pretty", "true");
                    response = restClient.performRequest(request);

                    if (response.getStatusLine().getStatusCode() == 200L) {
                        rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                        StringBuffer result = new StringBuffer();
                        while ((line = rd.readLine()) != null) {
                            result.append(line);
                        }
                        response.getEntity().getContent().close();
                        time = result.substring(result.indexOf("took") + 6, result.indexOf("timed_out") - 2);
                        String tmp = result.substring(result.indexOf("hits"));
                        records = tmp.substring(tmp.indexOf("total") + 7, tmp.indexOf("max_score") - 2);

                        LOG.info("queryString: " + introduce + place + hobby + " ,queryTime: " + time + " ,hitRecords: " + records);
                    }
                } catch (IOException e) {
                    LOG.error("Query failed.");
                }
            }
        }
    }

    private static void putAndQueryData(long recordNum, String threadinfo) throws Exception {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        String processPid = processName.split("@")[0];
        Map<String, Object> esJson = new HashMap<String, Object>();
        long idSeqNumber = 0L;
        long circleCommit = recordNum / 1000L;
        JSONObject json = null;
        String str = "{ \"index\" : { \"_index\" : \"" + configInfo.index + "\"} }";

        for (int j = 0; j < circleCommit; j++) {
            Date time = new Date();
            StringBuffer buffer = new StringBuffer();
            for (int i = 0; i < 1000; i++) {
                esJson.clear();
                idSeqNumber = idSeqNumber + 1L;
                idSeqNumber = idSeqNumber + 1L;
                String strId = processPid + "_" + threadinfo + "_" + idSeqNumber;
                int strLength = 72;
                esJson = esJsonInput(strId, strLength);
                json = JSONObject.fromObject(esJson);
                buffer.append(str).append("\n");
                buffer.append(json).append("\n");
            }

            StringEntity entity = new StringEntity(buffer.toString(), ContentType.APPLICATION_JSON);
            entity.setContentEncoding("UTF-8");
            Response response = null;
            try {
                Request request = new Request("PUT", "/_bulk");
                request.setEntity(entity);
                request.addParameter("pretty", "true");
                response = restClient.performRequest(request);
                if (response.getStatusLine().getStatusCode() == 200 ||
                        response.getStatusLine().getStatusCode() == 201) {
                    LOG.info(time + "==putData,response entity is : " + EntityUtils.toString(response.getEntity()));

                    int age = ThreadLocalRandom.current().nextInt(18, 100);
                    BufferedReader rd = null;
                    String line = "";
                    String querytime = "";
                    String records = "";
                    request = new Request("GET", "/" + configInfo.index + "/" + configInfo.type + "/_search?size=1&q=age:" + age);
                    request.addParameter("pretty", "true");
                    Response result = restClient.performRequest(request);
                    if (result.getStatusLine().getStatusCode() == 200L) {
                        rd = new BufferedReader(new InputStreamReader(result.getEntity().getContent()));
                        StringBuffer result1 = new StringBuffer();
                        while ((line = rd.readLine()) != null) {
                            result1.append(line);
                        }
                        result.getEntity().getContent().close();
                        querytime = result1.substring(result1.indexOf("took") + 6, result1.indexOf("timed_out") - 2);
                        String tmp = result1.substring(result1.indexOf("hits"));
                        records = tmp.substring(tmp.indexOf("total") + 7, tmp.indexOf("max_score") - 2);
                        LOG.info("queryString: " + age + " ,queryTime: " + querytime + " ,hitRecords: " + records);
                    }
                } else {
                    LOG.error("putData failed.");
                }
            } catch (Exception e) {
                LOG.error("putData failed.");
            }
        }
    }

    private static void startThreadInput(int threadNum) {
        Thread[] th = new Thread[threadNum];
        MultipleThInputRun[] arrayOfmultipleThInputRun = new MultipleThInputRun[threadNum];
        for (int i = 0; i < threadNum; i++) {
            arrayOfmultipleThInputRun[i] = new MultipleThInputRun();
        }
        for (int i = 0; i < threadNum; i++) {
            th[i] = new Thread(arrayOfmultipleThInputRun[i]);
        }
        for (int i = 0; i < threadNum; i++) {
            th[i].start();
        }
        for (int i = 0; i < threadNum; i++) {
            try {
                th[i].join();
            } catch (InterruptedException e) {
                LOG.error("Thread join failed.", e);
            }
        }
    }

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        Properties properties = new Properties();
        String proPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "espara.properties";
        try {
            properties.load(Files.newInputStream(Paths.get(proPath)));
        } catch (IOException e) {
            LOG.error("Failed to load properties.");
        }

        configInfo = new ConfigInfo.Builder().setHttpUrl(properties.getProperty("httpUrl"))
                .setProcessRecordNum(Long.parseLong(properties.getProperty("processRecordNum")))
                .setIndex(properties.getProperty("index"))
                .setType(properties.getProperty("type"))
                .setThreadNum(Integer.parseInt(properties.getProperty("threadNum")))
                .setIsSecurity(Integer.parseInt(properties.getProperty("isSecurity")))
                .setIsIndex(Integer.parseInt(properties.getProperty("isIndex")))
                .setConnectTimeout(Integer.parseInt(properties.getProperty("connectTimeout")))
                .setSocketTimeout(Integer.parseInt(properties.getProperty("socketTimeout")))
                .setSingleRecordData(Integer.parseInt(properties.getProperty("singleRecordData")))
                .setQueryType(properties.getProperty("queryType"))
                .build();
        threadCommitNum = configInfo.processRecordNum / configInfo.threadNum;
        try {
            restClient = getRestClient(getHostArray(configInfo.httpUrl));
            startThreadInput(configInfo.threadNum);
        } catch (Exception e) {
            LOG.error("Thread start failed.", e);
        } finally {
            if (restClient != null) {
                try {
                    restClient.close();
                    LOG.info("Close the client successful in main.");
                } catch (Exception e1) {
                    LOG.error("Close the client failed in main.", e1);
                }
            }
        }
        long endTime = System.currentTimeMillis();
        LOG.info("run time:" + ((endTime - startTime) / 1000.0D) + "S");
    }

    static class MultipleThInputRun implements Runnable {
        public void run() {
            try {
                if (configInfo.isIndex == 0) {
                    ESPerformanceMain.dataInput(threadCommitNum,
                            String.format("%d", Thread.currentThread().getId()));
                } else if (configInfo.isIndex == 1) {
                    if (configInfo.queryType.charAt(0) == 'a') {
                        ESPerformanceMain.queryByCondition(threadCommitNum, configInfo.queryType.charAt(1));
                    } else if (configInfo.queryType.charAt(0) == 'm') {
                        ESPerformanceMain.queryByBlurred(threadCommitNum, configInfo.queryType.charAt(1));
                    } else if (configInfo.queryType.charAt(0) == 's') {
                        ESPerformanceMain.queryByField(threadCommitNum, configInfo.queryType.charAt(1));
                    } else {
                        ESPerformanceMain.queryAtFixedRate(threadCommitNum);
                    }

                } else if (configInfo.isIndex == 2) {
                    ESPerformanceMain.putAndQueryData(threadCommitNum,
                            String.format("%d", Thread.currentThread().getId()));
                }
            } catch (Exception e) {
                LOG.error("Multiple Thread run failed.", e);
            }
        }
    }
}
