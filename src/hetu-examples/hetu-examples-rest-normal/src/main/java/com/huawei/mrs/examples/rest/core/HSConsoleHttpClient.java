/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.mrs.examples.rest.core;

import org.apache.http.client.CookieStore;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * The HSConsoleHttpClient
 *
 * @since 2020-05-01
 */
public class HSConsoleHttpClient {
    /**
     * get http client.
     *
     * @return http client
     * @throws Exception IOException or CasAuthException
     */
    public CloseableHttpClient getHttpClient() throws Exception {
        return HttpClientBuilder.create().build();
    }
}
