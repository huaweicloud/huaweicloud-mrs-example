/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.fusioninsight.lakesearch.example.client;

import com.huawei.fusioninsight.lakesearch.example.enums.MethodType;
import com.huawei.fusioninsight.lakesearch.example.enums.OperateType;
import com.huawei.fusioninsight.lakesearch.example.model.ResultModel;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.alibaba.fastjson.JSONObject;
import com.huawei.us.common.file.UsFileUtils;
import com.sun.istack.Nullable;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.HttpMultipartMode;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Optional;
import java.util.UUID;
import java.util.List;
import java.util.Collections;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;

public class LakeSearchClient {
    private static final Logger LOG = LoggerFactory.getLogger(LakeSearchClient.class);

    private static String confPath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;

    private final List<String> goodIps = new ArrayList<>();

    private final List<String> badIps = new ArrayList<>();

    private final SecureRandom random;

    public LakeSearchClient(String[] ipAddrs) {
        Collections.addAll(goodIps, ipAddrs);
        random = new SecureRandom();
    }

    public Optional<ResultModel> sendAction(Subject subject, String protocol, String ipPort,
                                            String endpoint, MethodType requestType, JsonElement requestContent,
                                            OperateType operateType, String uploadFileName) {
        PrivilegedAction<ResultModel> sendAction = () -> {
            ResultModel result = null;

            int index = 0;
            String badIp;
            boolean isRequestSuccess = false;
            do {
                try {
                    index = random.nextInt(goodIps.size());
                    result = call(protocol, goodIps.get(index), ipPort, endpoint, requestType, requestContent, operateType, uploadFileName);
                    isRequestSuccess = true;
                    break;
                } catch (NoSuchAlgorithmException | KeyManagementException | IOException e) {
                    LOG.error("Failed send request to {}.", goodIps.get(index));
                    isRequestSuccess = false;
                    badIp = goodIps.get(index);
                    goodIps.remove(badIp);
                    badIps.add(badIp);
                }
            } while (goodIps.size() > 0);

            result = retryIfGoodIpIsEmpty(protocol, ipPort, endpoint, requestType, requestContent, operateType, uploadFileName, result, index, isRequestSuccess);
            return result;
        };
        ResultModel result = Subject.doAs(subject, sendAction);
        return result == null ? Optional.empty() : Optional.of(result);
    }

    private ResultModel retryIfGoodIpIsEmpty(String protocol, String ipPort, String endpoint, MethodType requestType, JsonElement requestContent, OperateType operateType, String uploadFileName, ResultModel result, int index, boolean isRequestSuccess) {
        String goodIp;
        if (!isRequestSuccess) {
            int retryTimes = 0;
            while (retryTimes < 3) {
                try {
                    index = random.nextInt(badIps.size());
                    result = call(protocol, badIps.get(index), ipPort, endpoint, requestType, requestContent, operateType, uploadFileName);
                    goodIp = badIps.get(index);
                    badIps.remove(goodIp);
                    goodIps.add(goodIp);
                    break;
                } catch (NoSuchAlgorithmException | KeyManagementException | IOException e) {
                    LOG.error("Failed send request to {}.", badIps.get(index));
                    retryTimes++;
                }
            }
        }
        return result;
    }

    public ResultModel call(String protocol, String ipAddr, String ipPort, String endpoint, MethodType type,
                            JsonElement requestContent, OperateType operateType, String uploadFileName)
        throws NoSuchAlgorithmException, KeyManagementException, IOException {
        ResultModel result = new ResultModel();
        String url = protocol + ipAddr + ":" + ipPort + endpoint;
        try (CloseableHttpClient httpclient = getHttpClient()) {
            HttpUriRequest request;
            switch (type) {
                case GET:
                    request = new HttpGet(url);
                    break;
                case POST:
                    request = new HttpPost(url);
                    break;
                case DELETE:
                    request = new HttpDelete(url);
                    break;
                case PUT:
                    request = new HttpPut(url);
                    break;
                default:
                    throw new IOException("Invalid request type.");
            }

            if (requestContent != null && request instanceof HttpEntityEnclosingRequestBase) {
                StringEntity requestEntity = new StringEntity(requestContent.toString(), "UTF-8");
                ((HttpEntityEnclosingRequestBase) request).setEntity(requestEntity);
            }

            request.setHeader(HttpHeaders.ACCEPT, "application/json");
            request.setHeader(HttpHeaders.CONTENT_ENCODING, "UTF-8");
            request.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
            request.setHeader(HttpHeaders.ACCEPT_ENCODING, "identity");
            request.setHeader(HttpHeaders.CONNECTION, "keep-alive");
            setEntityforUpload(operateType, uploadFileName, request);

            HttpResponse response = httpclient.execute(request);
            int statusCode = response.getStatusLine().getStatusCode();
            handleResponse(operateType, result, response, statusCode, ipAddr);
        }
        return result;
    }

    private void handleResponse(OperateType operateType, ResultModel result, HttpResponse response, int statusCode, String ipAddr) throws IOException {
        HttpEntity entity = response.getEntity();
        String entityString = null;
        try {
            if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_NO_CONTENT || statusCode == HttpStatus.SC_CREATED) {
                result.setStatusCode(statusCode);
                if (entity != null) {
                    entityString = EntityUtils.toString(entity, "UTF-8");
                    if (operateType.toString().equals("CHAT")) {
                        JSONObject jsonObject = JSONObject.parseObject(entityString);
                        entityString = jsonObject.getString("chat_result");
                    }
                    result.setContent(JsonParser.parseString(entityString).getAsJsonObject());
                }
                LOG.info("IP:{}, {} request successed. Status: {}, Details: {}.", ipAddr, operateType, response.getStatusLine(),
                    entity == null ? "No more" : entityString);
            } else {
                LOG.error("IP:{}, {} request failed. Status: {}, Details: {}.", ipAddr, operateType, response.getStatusLine(),
                    entity == null ? "No more" : EntityUtils.toString(entity, "UTF-8"));
            }
        } finally {
            EntityUtils.consume(entity);
        }
    }

    private void setEntityforUpload(OperateType operateType, String uploadFileName, HttpUriRequest request) {
        if (operateType.toString().equals("UPLOAD_FILES") || operateType.toString().equals("BULK_IMPORT_FAQ")
            || operateType.toString().equals("UPLOAD_STRUCTURED_DATA")) {
            String boundary = UUID.randomUUID().toString();
            request.removeHeaders(HttpHeaders.CONTENT_TYPE);
            confPath = confPath.replace("\\", "\\\\");
            File file = UsFileUtils.getFile(confPath + uploadFileName);
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();
            builder.addPart("file", new FileBody(file));
            builder.setBoundary(boundary);
            builder.setMode(HttpMultipartMode.RFC6532);
            HttpEntity entity = builder.build();
            if (request instanceof HttpEntityEnclosingRequestBase) {
                ((HttpEntityEnclosingRequestBase) request).setEntity(entity);
            }
        }
    }

    public CloseableHttpClient getHttpClient() throws KeyManagementException, NoSuchAlgorithmException {
        CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(new AuthScope(null, -1, null), new Credentials() {
            @Override
            public String getPassword() {
                return null;
            }

            @Override
            public Principal getUserPrincipal() {
                return null;
            }
        });
        Registry<AuthSchemeProvider> authSchemeRegistry = RegistryBuilder.<AuthSchemeProvider>create().register(
            AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build();
        SSLContext sslcontext = createIgnoreVerifySSL();

        Registry<ConnectionSocketFactory> socketFactoryRegistry
            = RegistryBuilder.<ConnectionSocketFactory>create()
            .register("https", new SSLConnectionSocketFactory(sslcontext, NoopHostnameVerifier.INSTANCE))
            .register("http", PlainConnectionSocketFactory.getSocketFactory())
            .build();

        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        CloseableHttpClient httpclient = HttpClients.custom()
            .setDefaultAuthSchemeRegistry(authSchemeRegistry)
            .setDefaultCredentialsProvider(credsProvider)
            .setConnectionManager(connManager)
            .build();
        return httpclient;
    }

    @Nullable
    private SSLContext createIgnoreVerifySSL() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sc = SSLContext.getInstance("TLSv1.2");
        X509TrustManager trustManager = new X509TrustManager() {
            @Override
            public void checkClientTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate,
                                           String paramString) {
            }

            @Override
            public void checkServerTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate,
                                           String paramString) {
            }

            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };
        sc.init(null, new TrustManager[]{trustManager}, null);
        return sc;
    }
}
