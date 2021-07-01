package com.huawei.graphbase.rest.security;

import com.huawei.graphbase.rest.util.RestHelper;

import org.apache.commons.io.FileUtils;
import org.apache.http.Consts;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;

public class GraphHttpClient {
    public final String csrfToken;

    public final CloseableHttpClient httpClient;

    public final HttpAuthInfo httpAuthInfo;

    private GraphHttpClient(final HttpAuthInfo httpAuthInfo, final CloseableHttpClient httpClient,
        final String csrfToken) {
        this.httpAuthInfo = httpAuthInfo;
        this.csrfToken = csrfToken;
        this.httpClient = httpClient;
    }

    public static GraphHttpClient newPasswordClient(final HttpAuthInfo httpAuthInfo) throws Exception {
        CloseableHttpClient httpClient = WebClientDevWrapper.wrapClient("TLS");
        try {
            // step1. username password login
            basePasswordLogin(httpAuthInfo, httpClient);

            // step2. fetch CSRF Token
            String token = fetchToken(httpAuthInfo, httpClient);
            if (null == token) {
                return null;
            }

            return new GraphHttpClient(httpAuthInfo, httpClient, token);
        } catch (Exception e) {
            httpClient.close();
            throw e;
        }
    }

    public static GraphHttpClient newKeytabClient(final HttpAuthInfo httpAuthInfo) throws Exception {
        CloseableHttpClient httpClient = WebClientDevWrapper.wrapClient("TLS");
        try {
            // step1. username keytabFile login
            baseKeytabLogin(httpAuthInfo, httpClient);

            // step2. fetch CSRF Token
            String token = fetchToken(httpAuthInfo, httpClient);
            if (null == token) {
                return null;
            }

            return new GraphHttpClient(httpAuthInfo, httpClient, token);
        } catch (Exception e) {
            httpClient.close();
            throw e;
        }
    }

    private static String fetchToken(HttpAuthInfo httpAuthInfo, CloseableHttpClient httpClient) throws Exception {
        HttpGet httpGet = new HttpGet(httpAuthInfo.getBaseUrl() + "/graph/user/me");
        CloseableHttpResponse csrfTokenRsp = null;
        String csrfToken = null;

        try {
            csrfTokenRsp = httpClient.execute(httpGet);
            RestHelper.checkHttpRsp(csrfTokenRsp);

            String tokenRspText = EntityUtils.toString(csrfTokenRsp.getEntity());
            JSONObject tokenRspJson = new JSONObject(tokenRspText);
            csrfToken = tokenRspJson.getString("csrfToken")
                .toString()
                .substring(0, tokenRspJson.getString("csrfToken").toString().length());

            if ((null == csrfToken) || (csrfToken.length() <= 0)) {
                System.out.println("TOKEN[2/2]: fetch token fail. " + httpGet.getRequestLine());
                throw new Exception("fetch token fail.");
            } else {
                System.out.println(
                    "TOKEN[2/2]: fetch token succeed. " + httpGet.getRequestLine() + " CSRFTOKEN: " + csrfToken + "\n");
            }
        } finally {
            if (null != csrfTokenRsp) {
                try {
                    csrfTokenRsp.close();
                } catch (IOException e) {
                    // nothing to do
                }
            }
        }

        return csrfToken;
    }

    private static boolean baseKeytabLogin(HttpAuthInfo httpAuthInfo, CloseableHttpClient httpclient) throws Exception {
        CloseableHttpResponse baseRsp = null;
        CloseableHttpResponse loginRsp = null;
        boolean isSuccessfully = false;

        try {
            HttpPost loginPost = new HttpPost(httpAuthInfo.getBaseUrl() + "/login");
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
            entityBuilder.addPart("username",
                new StringBody(httpAuthInfo.getUsername(), ContentType.create("text/plain", Consts.UTF_8)));
            entityBuilder.addBinaryBody("keytabfile", FileUtils.getFile(httpAuthInfo.getKeytabFilePath()));
            loginPost.setEntity(entityBuilder.build());
            loginRsp = httpclient.execute(loginPost);
            try {
                RestHelper.checkHttpRsp(loginRsp);
            } finally {
                if (loginRsp != null) {
                    loginRsp.close();
                }
            }

            isSuccessfully = true;
            System.out.println("TOKEN[1/2]: base login succeed. " + loginPost.getRequestLine());
        } finally {
            if (null != baseRsp) {
                baseRsp.close();
            }
        }
        return isSuccessfully;
    }

    private static boolean basePasswordLogin(HttpAuthInfo httpAuthInfo, CloseableHttpClient httpclient)
        throws Exception {
        CloseableHttpResponse baseRsp = null;
        CloseableHttpResponse loginRsp = null;
        boolean isSuccessfully = false;

        try {
            HttpPost loginPost = new HttpPost(httpAuthInfo.getBaseUrl() + "/login");
            MultipartEntityBuilder entityBuilder = MultipartEntityBuilder.create();
            entityBuilder.addPart("username",
                new StringBody(httpAuthInfo.getUsername(), ContentType.create("text/plain", Consts.UTF_8)));
            entityBuilder.addPart("password",
                new StringBody(httpAuthInfo.getPassword(), ContentType.create("text/plain", Consts.UTF_8)));
            loginPost.setEntity(entityBuilder.build());
            loginRsp = httpclient.execute(loginPost);
            try {
                RestHelper.checkHttpRsp(loginRsp);
            } finally {
                if (null != loginRsp) {
                    loginRsp.close();
                }
            }

            isSuccessfully = true;
            System.out.println("TOKEN[1/2]: base login succeed. " + loginPost.getRequestLine());
        } finally {
            if (null != baseRsp) {
                baseRsp.close();
            }
        }
        return isSuccessfully;
    }

    public void close() throws IOException {
        if (null != this.httpClient) {
            this.httpClient.close();
        }

    }
}
