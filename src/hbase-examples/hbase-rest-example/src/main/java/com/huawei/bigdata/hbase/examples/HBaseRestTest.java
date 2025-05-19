/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.bigdata.hbase.examples;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.istack.Nullable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
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
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;


/**
 * Function description:
 *
 * hbase rest test class
 *
 * @since 2013
 */

public class HBaseRestTest {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseRestTest.class.getName());

    private static final int STATUS_CODE_SUCCESS = 200;

    private static String userKeytabFile;

    private static String krb5File;

    private static Subject subject;

    private static void login(String principal, String userKeytabFile, String krb5File) throws LoginException {
        Map<String, String> options = new HashMap<>();
        options.put("useTicketCache", "false");
        options.put("useKeyTab", "true");
        options.put("keyTab", userKeytabFile);

        /**
         * Krb5 in GSS API needs to be refreshed so it does not throw the error
         * Specified version of key is not available
         */

        options.put("refreshKrb5Config", "true");
        options.put("principal", principal);
        options.put("storeKey", "true");
        options.put("doNotPrompt", "true");
        options.put("isInitiator", "true");
        options.put("debug", "true");
        System.setProperty("java.security.krb5.conf", krb5File);
        Configuration config = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[] {
                    new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)
                };
            }
        };
        subject = new Subject(false, Collections.singleton(new KerberosPrincipal(principal)), Collections.EMPTY_SET,
            Collections.EMPTY_SET);
        LoginContext loginContext = new LoginContext("Krb5Login", subject, null, config);
        loginContext.login();
    }

    public static void main(String[] args) throws Exception {
        // Set absolute path of 'user.keytab' and 'krb5.conf'
        String userdir;
        String osName = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        if (osName.contains("windows")) {
            // In Windows environment
            userdir = HBaseRestTest.class.getClassLoader().getResource("conf").getPath() + File.separator;
            // For JDK 9+, not support the path starts with /, need remove it.
            if (userdir.startsWith("/")) {
                int length = userdir.length();
                userdir = userdir.substring(1, length);
            }
        } else {
            // In Linux environment
            userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        }

        userKeytabFile = userdir + "user.keytab";
        krb5File = userdir + "krb5.conf";

        String principal = "hbaseuser1";

        login(principal, userKeytabFile, krb5File);

        // RESTServer's hostname.
        String restHostName = "xxx.xxx.xxx.xxx";
        String securityModeUrl = new StringBuilder("https://").append(restHostName).append(":21309").toString();
        String nonSecurityModeUrl = new StringBuilder("http://").append(restHostName).append(":21309").toString();
        HBaseRestTest test = new HBaseRestTest();

        //If cluster is non-security mode，use nonSecurityModeUrl as parameter.
        test.test(securityModeUrl);
    }

    private void test(String url) {
        // Cluster info
        getClusterVersion(url);
        getClusterStatus(url);

        // List tables
        getAllUserTables(url);

        // Namespace operations.
        createNamespace(url, "testNs");
        getAllNamespace(url);
        deleteNamespace(url, "testNs");

        // Add a table with specified info.
        createTable(url, "testRest",
                "{\"name\":\"default:testRest\",\"ColumnSchema\":[{\"name\":\"cf1\"}," + "{\"name\":\"cf2\"}]}");

        getAllNamespaceTables(url, "default");

        // Add column family 'testCF1' if not exist, else update the 'VERSIONS' to 3.
        // Notes: The unspecified property of this column family will be updated to default value.
        modifyTable(url, "testRest",
            "{\"name\":\"testRest\",\"ColumnSchema\":[{\"name\":\"testCF1\"," + "\"VERSIONS\":\"3" + "\"}]}");

        // Describe specific Table.
        descTable(url, "default:testRest");

        // delete a table with specified info.
        deleteTable(url, "default:testRest",
            "{\"name\":\"default:testRest\",\"ColumnSchema\":[{\"name\":\"testCF\"," + "\"VERSIONS\":\"3\"}]}");
    }

    private void modifyTable(String url, String tableName, String jsonHTD) {
        LOG.info("Start modify table.");
        String endpoint = "/" + tableName + "/schema";
        JsonElement tableDesc = new JsonParser().parse(jsonHTD);

        // Add a new column family or modify it.
        handleNormalResult(sendAction(url + endpoint, MethodType.POST, tableDesc));
    }

    private void createTable(String url, String tableName, String jsonHTD) {
        LOG.info("Start create table.");
        String endpoint = "/" + tableName + "/schema";
        JsonElement tableDesc = new JsonParser().parse(jsonHTD);

        // Add a table.
        handleCreateTableResult(sendAction(url + endpoint, MethodType.PUT, tableDesc));
    }

    private void deleteTable(String url, String tableName, String jsonHTD) {
        LOG.info("Start delete table.");
        String endpoint = "/" + tableName + "/schema";
        JsonElement tableDesc = new JsonParser().parse(jsonHTD);

        // Add a table.
        handleNormalResult(sendAction(url + endpoint, MethodType.DELETE, tableDesc));
    }

    private void descTable(String url, String tableName) {
        String endpoint = "/" + tableName + "/schema";
        Optional<ResultModel> result = sendAction(url + endpoint, MethodType.GET, null);
        handleNormalResult((Optional<ResultModel>) result);
    }

    private void getClusterVersion(String url) {
        String endpoint = "/version/cluster";
        Optional<ResultModel> result = sendAction(url + endpoint, MethodType.GET, null);
        handleNormalResult((Optional<ResultModel>) result);
    }

    private void handleNormalResult(Optional<ResultModel> result) {
        if (result.isPresent() && result.get().getStatusCode() == HttpStatus.SC_OK) {
            JsonObject content = result.get().getContent();
            LOG.info(content != null ? content.toString() : "Process success.");
        } else {
            LOG.error("Process failed with status code: {}", result.orElse(new ResultModel()).getStatusCode());
        }
    }

    private void handleCreateTableResult(Optional<ResultModel> result) {
        if (result.isPresent() && result.get().getStatusCode() == HttpStatus.SC_CREATED) {
            JsonObject content = result.get().getContent();
            LOG.info(content != null ? content.toString() : "Process success.");
        } else {
            LOG.error("Process failed with status code: {}", result.orElse(new ResultModel()).getStatusCode());
        }
    }

    private void getClusterStatus(String url) {
        String endpoint = "/status/cluster";
        Optional<ResultModel> result = sendAction(url + endpoint, MethodType.GET, null);
        handleNormalResult(result);
    }

    private void getAllUserTables(String url) {
        String endpoint = "/";
        Optional<ResultModel> result = sendAction(url + endpoint, MethodType.GET, null);
        handleNormalResult(result);
    }

    private void getAllNamespace(String url) {
        String endpoint = "/namespaces";
        Optional<ResultModel> result = sendAction(url + endpoint, MethodType.GET, null);
        handleNormalResult(result);
    }

    private void createNamespace(String url, String namespace) {
        String endpoint = "/namespaces/" + namespace;
        Optional<ResultModel> result = sendAction(url + endpoint, MethodType.POST, null);
        if (result.orElse(new ResultModel()).getStatusCode() == HttpStatus.SC_CREATED) {
            LOG.info("Create namespace '{}' success.", namespace);
        } else {
            LOG.error("Create namespace '{}' failed.", namespace);
        }
    }

    private void deleteNamespace(String url, String namespace) {
        String endpoint = "/namespaces/" + namespace;
        Optional<ResultModel> result = sendAction(url + endpoint, MethodType.DELETE, null);
        if (result.orElse(new ResultModel()).getStatusCode() == HttpStatus.SC_OK) {
            LOG.info("Delete namespace '{}' success.", namespace);
        } else {
            LOG.error("Delete namespace '{}' failed.", namespace);
        }
    }

    private void getAllNamespaceTables(String url, String namespace) {
        String endpoint = "/namespaces/" + namespace + "/tables";
        Optional<ResultModel> result = sendAction(url + endpoint, MethodType.GET, null);
        handleNormalResult(result);
    }

    private Optional<ResultModel> sendAction(String url, MethodType requestType, JsonElement requestContent) {
        PrivilegedAction<ResultModel> sendAction = () -> {
            ResultModel result = null;
            try {
                result = call(url, requestType, requestContent);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                LOG.error("Failed create SSL connection.", e);
            } catch (IOException e) {
                LOG.error("Failed send request.", e);
            }
            return result;
        };
        ResultModel result = Subject.doAs(subject, sendAction);
        return result == null ? Optional.empty() : Optional.of(result);
    }

    private ResultModel call(String url, MethodType type, JsonElement requestContent)
        throws NoSuchAlgorithmException, KeyManagementException, IOException {
        ResultModel result = new ResultModel();
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
            HttpResponse response = httpclient.execute(request);

            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = null;
            try {
                entity = response.getEntity();
                if (statusCode == HttpStatus.SC_OK || statusCode == HttpStatus.SC_NO_CONTENT
                    || statusCode == HttpStatus.SC_CREATED) {
                    result.setStatusCode(statusCode);
                    if (entity != null && entity.getContentLength() > 0) {
                        result.setContent(new JsonParser().parse(EntityUtils.toString(entity)).getAsJsonObject());
                    }
                } else {
                    LOG.error("Request failed. Status: {}, Details: {}.", response.getStatusLine(),
                        entity == null ? "No more" : EntityUtils.toString(entity, "UTF-8"));
                }
            } finally {
                EntityUtils.consume(entity);
            }
        }
        return result;
    }

    @Nullable
    private SSLContext createIgnoreVerifySSL() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext sc = SSLContext.getInstance("TLSv1.2");
        X509TrustManager trustManager = new X509TrustManager() {
            @Override
            public void checkClientTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate,
                String paramString) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(java.security.cert.X509Certificate[] paramArrayOfX509Certificate,
                String paramString) throws CertificateException {
            }

            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };
        sc.init(null, new TrustManager[] {trustManager}, null);
        return sc;
    }

    @Nullable
    private CloseableHttpClient getHttpClient() throws KeyManagementException, NoSuchAlgorithmException {
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
}

enum MethodType {
    GET,
    POST,
    DELETE,
    PUT
}

class ResultModel {
    private int statusCode;

    private JsonObject content;

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }

    public JsonObject getContent() {
        return content;
    }

    public void setContent(JsonObject content) {
        this.content = content;
    }
}
