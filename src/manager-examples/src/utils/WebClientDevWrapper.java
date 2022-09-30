package utils;

import org.apache.http.client.HttpClient;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * WebClientDevWrapper
 *
 * @author huawei
 * @version [V100R002C30, 2014-09-09]
 * @since [OM 1.0]
 */
public class WebClientDevWrapper {
    private static final String PROTOCOL_NAME = "https";

    private static final int PORT = 443;

    private static final String DEFAULT_SSL_VERSION = "TLS";

    private static final Logger LOG = LoggerFactory.getLogger(WebClientDevWrapper.class);

    /**
     * wrapClient
     *
     * @param base           HttpClient
     * @param userTLSVersion String
     * @return HttpClient
     */
    public static HttpClient wrapClient(HttpClient base, String userTLSVersion) {
        SSLContext sslContext = null;
        try {
            sslContext = SSLContext.getInstance(userTLSVersion);
        } catch (NoSuchAlgorithmException e1) {
            LOG.error("NoSuchAlgorithmException, msg is:{}.", e1.getMessage());
            return null;
        }

        if (sslContext == null) {
            return null;
        }
        X509TrustManager trustManager = new X509TrustManager() {
            /**
             * getAcceptedIssuers
             *
             * @return X509Certificate[]
             */
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            /**
             * checkClientTrusted
             *
             * @param ax509certificate X509Certificate
             * @param s String
             *
             * @throws CertificateException 异常
             */
            public void checkClientTrusted(X509Certificate[] ax509certificate, String s) throws CertificateException {
            }

            /**
             * checkServerTrusted
             *
             * @param ax509certificate X509Certificate
             * @param s  String
             *
             * @throws CertificateException 异常
             */
            public void checkServerTrusted(X509Certificate[] ax509certificate, String s) throws CertificateException {
            }
        };
        try {
            sslContext.init(null, new TrustManager[] {trustManager}, new SecureRandom());
        } catch (KeyManagementException e) {
            LOG.error("Key Management Exception, msg is:{}.", e.getMessage());
        }
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
