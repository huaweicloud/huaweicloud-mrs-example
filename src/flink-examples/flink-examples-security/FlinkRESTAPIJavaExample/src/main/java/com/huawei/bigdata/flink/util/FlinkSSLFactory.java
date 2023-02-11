/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.util;

import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.security.GeneralSecurityException;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

/**
 * This factory is used to configure HTTPS in Hadoop HTTP based endpoints
 * 
 * @since 2020/10/10
 */
public class FlinkSSLFactory implements ConnectionConfigurator {
    /**
     * Returns a configured SSLSocketFactory.
     *
     * @return the configured SSLSocketFactory.
     * @throws GeneralSecurityException thrown if the SSLSocketFactory could not
     *                                  be initialized.
     * @throws IOException              thrown if and IO error occurred while loading
     *                                  the server keystore.
     */
    private SSLSocketFactory createSSLSocketFactory() throws GeneralSecurityException, IOException {
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, new TrustManager[] {new AnyTrust()}, null);
        return sslContext.getSocketFactory();
    }

    /**
     * Returns the hostname verifier it should be used in HttpsURLConnections.
     *
     * @return the hostname verifier.
     */
    private HostnameVerifier getHostnameVerifier() {
        return new AnyVerifier();
    }

    /**
     * If the given {@link HttpURLConnection} is an {@link HttpsURLConnection}
     * configures the connection with the {@link SSLSocketFactory} and
     * {@link HostnameVerifier} of this SqoopSSLFactory, otherwise does nothing.
     *
     * @param conn the {@link HttpURLConnection} instance to configure.
     * @return the configured {@link HttpURLConnection} instance.
     * @throws IOException if an IO error occurred.
     */
    @Override
    public HttpURLConnection configure(HttpURLConnection conn) throws IOException {
        if (conn instanceof HttpsURLConnection) {
            HttpsURLConnection sslConn = (HttpsURLConnection) conn;
            try {
                sslConn.setSSLSocketFactory(createSSLSocketFactory());
            } catch (GeneralSecurityException ex) {
                throw new IOException(ex);
            }

            sslConn.setHostnameVerifier(getHostnameVerifier());
            conn = sslConn;
        }
        return conn;
    }

    private class AnyVerifier implements HostnameVerifier {

        /**
         * 检验hostname
         * 
         * @param hostname 主机名 
         * @param session sslsession
         * @return boolean
         */
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    }
}
