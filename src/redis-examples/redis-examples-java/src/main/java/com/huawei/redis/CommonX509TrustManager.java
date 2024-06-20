/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2023-2023. All rights reserved.
 */

package com.huawei.redis;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.X509Certificate;
import java.util.Date;

import javax.net.ssl.X509TrustManager;

public class CommonX509TrustManager implements X509TrustManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonX509TrustManager.class);
    private static final String JAVA_HOME = "JAVA_HOME";
    private static final String JAVA_CACERTS = "/jre/lib/security/cacerts";
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {

    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return new X509Certificate[0];
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        checkTrustedParams(chain, authType);
        checkCertificateCaCert(chain);
        checkCertsValidity(chain);
    }

    /**
     * 检查checkServerTrusted相关参数
     *
     * @param chain    证书链
     * @param authType 认证类型
     * @throws CertificateException 检查异常
     */
    private void checkTrustedParams(X509Certificate[] chain, String authType) throws CertificateException {
        if (chain == null || chain.length == 0) {
            LOGGER.error("Null or zero-length certificate chain.");
            throw new CertificateException("Null or zero-length certificate chain.");
        }
        if (StringUtils.isEmpty(authType)) {
            LOGGER.error("Null or zero-length authType.");
            throw new CertificateException("Null or zero-length authType.");
        }
    }

    /**
     * 检查ca证书是否信任，不可信或者路径错误抛出异常。
     *
     * @param chain 证书链
     * @throws CertificateException 证书不可信，抛出异常
     */
    private void checkCertificateCaCert(X509Certificate[] chain) throws CertificateException {
        String cacertsPath = System.getenv(JAVA_HOME) + JAVA_CACERTS;
        File cacertsFile = FileUtils.getFile(cacertsPath);
        if (!cacertsFile.exists()) {
            LOGGER.error("Get trustStorePath failed.");
            throw new CertificateException("Get trustStorePath failed.");
        }
        boolean trusted;
        int size = chain.length;
        try (FileInputStream stream = FileUtils.openInputStream(FileUtils.getFile(cacertsPath))) {
            // 执行keytool命令，需要环境JDK密码,需要根据真实环境进行修改
            // keytool -import -alias openpayment -keystore ${java_home}/jre/lib/security/cacerts -file 证书路径/openpayment.cer
            // ******为环境JDK密码,根据实际环境进行修改，密码明文存储存在安全风险，建议在配置文件或者环境变量中密文存放，使用时解密，确保安全
            String decryptPwd = "******";
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(stream, decryptPwd.toCharArray());
            trusted = (trustStore.getCertificateAlias(chain[size - 1]) != null);
        } catch (IOException e) {
            LOGGER.error("read trustStore failed, {}", e.getMessage());
            throw new CertificateException("read trustStore failed");
        } catch (KeyStoreException | NoSuchAlgorithmException e) {
            LOGGER.error("Get trustStore or get cert alias name failed.", e);
            throw new CertificateException("Get trustStore or get cert alias name failed.");
        }
        if (!trusted) {
            LOGGER.error("The ca.crt is not in trustStore.");
            throw new CertificateException("The ca.crt is not in trustStore.");
        }
    }

    /**
     * 检查证书有效期
     *
     * @param chain 证书链
     * @throws CertificateException 有效期检查异常
     */
    private void checkCertsValidity(X509Certificate[] chain) throws CertificateException {
        Date now = new Date();
        X509Certificate cert = chain[chain.length - 1];
        try {
            cert.checkValidity(now);
        } catch (CertificateNotYetValidException e) {
            LOGGER.error("Certificate is not yet valid.", e);
            throw new CertificateException("Certificate is not yet valid.");
        } catch (CertificateExpiredException e) {
            LOGGER.error("Certificate Expired", e);
            throw new CertificateException("Certificate Expired");
        }
    }

}
