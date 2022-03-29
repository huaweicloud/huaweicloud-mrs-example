package com.huawei.graphbase.rest.security;

import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.params.HttpParams;

import java.io.IOException;
import java.net.Socket;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;

public class BigdataSslSocketFactory extends SSLSocketFactory {
    private static String[] enabelPro = {"TLSv1.2"};

    public BigdataSslSocketFactory(SSLContext sslContext, X509HostnameVerifier hostnameVerifier,
        String userTLSVersion) {
        super(sslContext, hostnameVerifier);
        enabelPro[0] = userTLSVersion;
    }

    @Override
    public Socket createSocket(HttpParams params) throws IOException {
        Socket result = super.createSocket(params);
        ((SSLSocket) result).setEnabledProtocols(enabelPro);
        return result;
    }

}
