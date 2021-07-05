package com.huawei.graphbase.rest.security;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class WebClientDevWrapper {
    private static final String PROTOCOL_NAME = "https";

    private static final int PORT = 443;

    private static final String DEFAULT_SSL_VERSION = "TLS";

    private static final int SessionCacheSize = 100;

    private static final int SessionTimeout = 60;

    public static DefaultHttpClient wrapClient(String userTLSVersion)
        throws NoSuchAlgorithmException, KeyManagementException {
        ThreadSafeClientConnManager connManager = new ThreadSafeClientConnManager();
        connManager.setMaxTotal(100);
        HttpClient base = new DefaultHttpClient(connManager);
        SSLContext sslContext = SSLContext.getInstance(userTLSVersion);
        X509TrustManager trustManager = new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted(X509Certificate[] ax509certificate, String s) throws CertificateException {
            }

            public void checkServerTrusted(X509Certificate[] ax509certificate, String s) throws CertificateException {
            }
        };
        //Optimize and modify the parameters SessionCacheSize and SessionTimeout according to the environment and the number of business requests.
        sslContext.getServerSessionContext().setSessionCacheSize(SessionCacheSize);
        sslContext.getServerSessionContext().setSessionTimeout(SessionTimeout);
        sslContext.init(null, new TrustManager[] {trustManager}, new SecureRandom());

        SSLSocketFactory sslSocketFactory = null;
        if ((userTLSVersion == null) || (userTLSVersion.isEmpty()) || (userTLSVersion.equals(DEFAULT_SSL_VERSION))) {
            sslSocketFactory = new SSLSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
        } else {
            sslSocketFactory = new BigdataSslSocketFactory(sslContext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER,
                userTLSVersion);
        }
        ClientConnectionManager ccm = base.getConnectionManager();
        SchemeRegistry sr = ccm.getSchemeRegistry();
        sr.register(new Scheme(PROTOCOL_NAME, PORT, sslSocketFactory));
        return new DefaultHttpClient(ccm, base.getParams());
    }
}
