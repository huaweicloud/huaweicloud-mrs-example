/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.Callable;

/**
 * 认证客户端
 * 
 * @since 2020/10/10
 */
public class AuthClient {
    private static final Logger LOG = LoggerFactory.getLogger(AuthClient.class);
    private String token;

    private boolean isLogin = false;

    private String url;

    private String principal;

    private String keytab;

    private final Object synObject = new Object();

    private static AuthClient authentication = new AuthClient();

    private AuthClient() {
    }

    /**
     * @return 单例对象
     */
    public static AuthClient getInstance() {
        return authentication;
    }

    public void authenticate(String url) throws IOException, AuthenticationException {
        AuthenticatedURL.Token authToken = new AuthenticatedURL.Token();
        try {
            ConnectionConfigurator connConf = new FlinkSSLFactory();
            KerberosAuthenticator kerberos = new KerberosAuthenticator();
            kerberos.setConnectionConfigurator(connConf);
            kerberos.authenticate(new URL(url), authToken);
        } catch (Exception e) {
            throw e;
        }

        this.token = tokenToStr(authToken);

        isLogin = true;
    }

    private String tokenToStr(AuthenticatedURL.Token token) {
        String tokenStr = token.toString();
        if (tokenStr != null) {
            if (!tokenStr.startsWith("\"")) {
                tokenStr = "\"" + tokenStr + "\"";
            }

            return tokenStr;
        }
        return null;
    }

    /**
     * login server to get token
     */
    public void login() throws Exception {
        synchronized (synObject) {
            try {
                KerberosUtils.doAsClient(
                        principal,
                        keytab,
                        new Callable<Void>() {
                            @Override
                            public Void call() throws IOException, AuthenticationException {
                                authenticate(url);
                                return null;
                            }
                        });
            } catch (Exception e) {
                throw e;
            }
        }
    }

    public String getToken() {
        return token;
    }

    /**
     * verify login
     * 
     * @return boolean
     */
    public boolean isLogin() {
        return isLogin;
    }

    /**
     * save the configure
     * 
     * @param url 校验url
     * @param principal 认证用户
     * @param keytab keytab文件
     */
    public void setConfigure(String url, String principal, String keytab) throws Exception {
        if (StringUtils.isEmpty(url) || StringUtils.isEmpty(principal) || StringUtils.isEmpty(keytab)) {
            return;
        }

        this.url = url;
        this.principal = principal;
        this.keytab = keytab;
    }
}
