/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.util;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.X509TrustManager;

/**
 * HTTPS信任所有证书
 * 
 * @since 2020/10/10
 */
public class AnyTrust implements X509TrustManager {
    
    /**
     * 检查客户端证书
     * 
     * @param chain  证书链
     * @param authType 认证类型
     * @throws CertificateException
     */
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    /**
     * 检查服务端证书
     * 
     * @param chain 证书链
     * @param authType 认证类型
     * @throws CertificateException
     */
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
    }

    /**
     * @return X509Certificate
     */
    public X509Certificate[] getAcceptedIssuers() {
        return null;
    }
}
