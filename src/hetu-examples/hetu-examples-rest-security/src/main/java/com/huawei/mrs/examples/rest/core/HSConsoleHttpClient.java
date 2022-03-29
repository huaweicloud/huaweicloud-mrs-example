/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.mrs.examples.rest.core;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.CookieStore;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * The HSConsoleHttpClient
 *
 * @since 2020-05-01
 */
public class HSConsoleHttpClient {
    private static final int STRING_UNFOUND = -1;


    private String getPageIt(HttpEntity entity) throws IOException {
        BufferedReader rd = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-8"));
        String tempLine = rd.readLine();
        String itFlag = "<input type=\"hidden\" name=\"lt\" value=\"";
        String lt = "";
        while (tempLine != null) {
            int index = tempLine.indexOf(itFlag);
            if (index != STRING_UNFOUND) {
                String s1 = tempLine.substring(index + itFlag.length());
                int index1 = s1.indexOf("\"");
                if (index1 != STRING_UNFOUND) {
                    lt = s1.substring(0, index1);
                }
            }
            tempLine = rd.readLine();
        }
        return lt;
    }

    /**
     * close the http client.
     *
     * @param httpClient The http client need to be closed.
     */
    public void closeClient(CloseableHttpClient httpClient) {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                System.out.println("httpClient clone fail.");
            }
        }
    }

    /**
     * close the http response.
     *
     * @param response The http client need to be closed.
     */
    public void closeResponse(CloseableHttpResponse response) {
        if (response != null) {
            try {
                response.close();
            } catch (IOException e) {
                System.out.println("response clone fail.");
            }
        }
    }

    /**
     * login fi cas.
     *
     * @param httpClient The http client.
     * @param lt login ticket
     * @param casUrl cas url
     * @param hsconsoleEndPoint hsconsole url
     * @param context http context
     */
    private boolean loginCas(CloseableHttpClient httpClient, String lt, String casUrl, String hsconsoleEndPoint, HttpClientContext context, String user, String passworld) throws Exception {
        List<NameValuePair> nvps = new ArrayList<>();
        nvps.add(new BasicNameValuePair("username", user));
        nvps.add(new BasicNameValuePair("password", passworld));
        nvps.add(new BasicNameValuePair("lt", lt));
        nvps.add(new BasicNameValuePair("_eventId", "submit"));
        nvps.add(new BasicNameValuePair("submit", "Login"));
        HttpEntity entity1 = new UrlEncodedFormEntity(nvps, "UTF-8");
        CloseableHttpResponse casResponse = (CloseableHttpResponse) HttpUtils.httpPost(httpClient, casUrl, entity1, null, context, null);
        closeResponse(casResponse);
        CloseableHttpResponse hsConsoleResponse = (CloseableHttpResponse) HttpUtils.httpGet(httpClient, hsconsoleEndPoint, null, null, context);
        String hsConsoleHtml = EntityUtils.toString(hsConsoleResponse.getEntity());
        if (hsConsoleHtml.contains("script src=\"runtime") && hsConsoleHtml.contains("script src=\"polyfills")) {
            System.out.println("login hsconsole success.");
            closeResponse(hsConsoleResponse);
            return true;
        } else {
            System.out.println("login hsconsole fail.");
            closeResponse(hsConsoleResponse);
            return false;
        }
    }

    /**
     * get http client.
     *
     * @param casUrl cas url.
     * @param hsconsoleEndpoint hsconsole url
     * @param user user
     * @param passworld passworld
     * @return http client
     * @throws Exception IOException or CasAuthException
     */
    public CloseableHttpClient getHttpClient(String casUrl, String hsconsoleEndpoint, String user, String passworld) throws Exception {
        CookieStore cookieStore = new BasicCookieStore();
        HttpClientContext context = HttpClientContext.create();
        context.setCookieStore(cookieStore);

        CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultCookieStore(cookieStore).build();
        CloseableHttpResponse response = (CloseableHttpResponse) HttpUtils.httpGet(httpClient, hsconsoleEndpoint, null, null, context);

        String pageIt = getPageIt(response.getEntity());
        if (pageIt.isEmpty()) {
            System.out.println(String.format("cannot get page it from %s, respone code is %s, response entity is %s.", hsconsoleEndpoint, response.getStatusLine().getStatusCode(), EntityUtils.toString(response.getEntity())));
            closeResponse(response);
            closeClient(httpClient);
            throw new RuntimeException(hsconsoleEndpoint + " cas authentication failed.");
        }

        if (loginCas(httpClient, pageIt, casUrl, hsconsoleEndpoint, context, user, passworld)) {
            return httpClient;
        } else {
            closeClient(httpClient);
            throw new RuntimeException("cas authentication failed.");
        }
    }
}
