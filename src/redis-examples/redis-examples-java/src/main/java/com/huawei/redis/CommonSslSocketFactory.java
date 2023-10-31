/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.redis;

import com.huawei.us.common.random.UsSecureRandom;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class CommonSslSocketFactory {
    private CommonSslSocketFactory() {
    }

    /**
     * 创建信任指定证书的SocketFactory
     *
     * @return 信任指定证书的SocketFactory
     * @throws NoSuchAlgorithmException NoSuchAlgorithmException
     * @throws KeyManagementException   KeyManagementException
     */
    public static SSLSocketFactory createSslSocketFactory() throws NoSuchAlgorithmException, KeyManagementException {
        TrustManager[] unTrustManagers = new TrustManager[]{new CommonX509TrustManager()};
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(null, unTrustManagers, SecureRandom.getInstanceStrong());
        return sslContext.getSocketFactory();
    }

    /**
     * 创建信任所有的ssl证书的Factory
     * @return 信任所有的ssl证书的Factory
     * @throws NoSuchAlgorithmException  NoSuchAlgorithmException
     * @throws KeyManagementException KeyManagementException
     */
    public static SSLSocketFactory createTrustALLSslSocketFactory()
        throws NoSuchAlgorithmException, KeyManagementException {
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
        sslContext.init(null, unTrustManagers, UsSecureRandom.getInstance());
        return sslContext.getSocketFactory();
    }

}
