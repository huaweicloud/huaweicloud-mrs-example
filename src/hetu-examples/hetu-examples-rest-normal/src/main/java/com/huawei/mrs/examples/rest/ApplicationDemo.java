/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.mrs.examples.rest;

import com.huawei.mrs.examples.rest.core.HSConsoleHttpClient;
import com.huawei.mrs.examples.rest.core.HttpUtils;
import com.huawei.mrs.examples.rest.model.ComputerInstanceConfig;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class ApplicationDemo {

    public static void main(String[] args) throws Exception {
        System.setProperty("javax.net.ssl.trustStore", ApplicationDemo.class.getClassLoader().getResource("hsconsole.jks").getPath());

        ApplicationDemo applicationDemo = new ApplicationDemo();
        Properties properties = applicationDemo.initConfig();
        String hsconsoleEndPoint = properties.getProperty("hsconsoleEndpoint");

        HSConsoleHttpClient hsConsoleHttpClient = new HSConsoleHttpClient();
        CloseableHttpClient httpClient = hsConsoleHttpClient.getHttpClient();

        // 测试创建计算实例
        //  applicationDemo.testCreateComputerInstance(httpClient, hsconsoleEndPoint);

        // 测试获取计算实例的信息
          applicationDemo.testGetComputerInstancesInfo(httpClient, hsconsoleEndPoint);

        // 测试获取实例的所有查询
        // applicationDemo.testGetAllRunningQuery(httpClient, hsconsoleEndPoint);

        // 测试查杀接口
        // applicationDemo.testKillSpecifiedQuery(httpClient, hsconsoleEndPoint, "20220314_123715_00042_mk3db@default@HetuEngine");

        // 测试扩缩容计算实例
        // applicationDemo.testFlexComputerInstance(httpClient, hsconsoleEndPoint, "99bd9815422245ce92e5628b2c99dba9");
    }

    private void handleResponseResult(CloseableHttpResponse response, String operation) throws IOException {
        if (response.getStatusLine().getStatusCode() == 200) {
            System.out.println(operation + " success.");
            InputStream in = response.getEntity().getContent();
            String result = IOUtils.toString(in, StandardCharsets.UTF_8);
            System.out.println(result);
        } else {
            System.out.println(operation + " fail.");
            InputStream in = response.getEntity().getContent();
            String responseDetail = IOUtils.toString(in, StandardCharsets.UTF_8);
            System.out.println(responseDetail);
        }
    }

    private void testCreateComputerInstance(CloseableHttpClient authedHttpClient, String hsconsoleEndpoint) throws Exception {
        String resourcePath = "/v1/hsconsole/clusters/config";
        String createUrl = hsconsoleEndpoint + resourcePath;

        String config = ComputerInstanceConfig.getCreateComputerInstanceConfigExample();
        HttpEntity entity = new StringEntity(config, ContentType.APPLICATION_JSON);
        Header[] headsRequest = {new BasicHeader("Content-Type", "application/json;charset=UTF-8")};

        try (CloseableHttpResponse response = (CloseableHttpResponse) HttpUtils.httpPost(authedHttpClient, createUrl, entity, null, null, headsRequest)) {
            handleResponseResult(response, "create computer instance");
        }
    }

    private void testGetAllRunningQuery(CloseableHttpClient authedHttpClient, String hsconsoleEndpoint) throws Exception {
        String resourcePath = "/v1/hsconsole/query/running/default";
        String getQueryUrl = hsconsoleEndpoint + resourcePath;

        try (CloseableHttpResponse response = (CloseableHttpResponse) HttpUtils.httpGet(authedHttpClient, getQueryUrl, null, null, null)) {
            handleResponseResult(response, "get all running queries");
        }
    }

    private void testKillSpecifiedQuery(CloseableHttpClient authedHttpClient, String hsconsoleEndpoint, String queryId) throws Exception {
        String resourcePath = "/v1/hsconsole/query/kill/" + queryId;
        String getQueryUrl = hsconsoleEndpoint + resourcePath;

        Header[] headsRequest = {new BasicHeader("Content-Type", "application/json;charset=UTF-8")};
        try (CloseableHttpResponse response = (CloseableHttpResponse) HttpUtils.httpPost(authedHttpClient, getQueryUrl, null, null, null, headsRequest)) {
            handleResponseResult(response, "kill specified query");
        }
    }

    private void testFlexComputerInstance(CloseableHttpClient authedHttpClient, String hsconsoleEndpoint, String computerClusterId) throws Exception {
        String resourcePath = "/v1/hsconsole/clusters/" + computerClusterId + "/flex";
        String flexComputerInstanceUrl = hsconsoleEndpoint + resourcePath;

        String config = ComputerInstanceConfig.getFlexComputerInstanceConfigExample();
        HttpEntity entity = new StringEntity(config, ContentType.APPLICATION_JSON);
        Header[] headsRequest = {new BasicHeader("Content-Type", "application/json;charset=UTF-8")};
        try (CloseableHttpResponse response = (CloseableHttpResponse) HttpUtils.httpPut(authedHttpClient, flexComputerInstanceUrl, entity, null, null, headsRequest)) {
            handleResponseResult(response, "flex computer instance");
        }
    }

    private void testGetComputerInstancesInfo(CloseableHttpClient authedHttpClient, String hsconsoleEndpoint) throws Exception {
        String resourcePath = "/v1/hsconsole/clusters?status=&sort=&size=&page=&filterType=&filterContent=&condition=";
        String getComputerInstancesInfo = hsconsoleEndpoint + resourcePath;

        try (CloseableHttpResponse response = (CloseableHttpResponse) HttpUtils.httpGet(authedHttpClient, getComputerInstancesInfo, null,  null, null)) {
            handleResponseResult(response, "get computer instance info");
        }
    }

    private Properties initConfig() throws IOException {
        Properties pro = new Properties();
        String applicationPropertiesFilePath = ApplicationDemo.class.getClassLoader().getResource("application.properties").getPath();
        FileInputStream in = new FileInputStream(applicationPropertiesFilePath);
        pro.load(in);
        in.close();
        return pro;
    }
}
