/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2021-2021. All rights reserved.
 */

package com.huawei.redis;

import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * @version 1.0
 * @apiNote
 * @since 2021-06-16
 */
public final class SSLSocketFactoryUtil {

    private SSLSocketFactoryUtil() {
    }

    /**
     * Creates an SSLSocketFactory with a trust manager that does not trust any
     * certificates.
     */
    public static SSLSocketFactory createTrustALLSslSocketFactory() throws Exception {
        TrustManager[] unTrustManagers = new TrustManager[] {
            new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() {
                    return new X509Certificate[0];
                }

                public void checkClientTrusted(X509Certificate[] chain, String authType) {

                }

                public void checkServerTrusted(X509Certificate[] chain, String authType) {

                }
            }
        };
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, unTrustManagers, new SecureRandom());
        return sslContext.getSocketFactory();
    }

}
