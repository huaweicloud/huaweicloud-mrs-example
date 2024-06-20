/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.util;

import org.apache.commons.lang3.StringUtils;

/**
 * 登录客户端
 * 
 * @since 2020/10/10
 */
public class LoginClient {
    private String url;

    private String user;

    private String password;

    private boolean usekeytab;

    private String principal;

    private String keytab;

    private String token;

    private boolean isLogin = false;

    /**
     * Token prefix.
     */
    private static final String TOKEN_PREFIX = "hadoop.auth=";

    private static LoginClient loginClient = new LoginClient();

    private LoginClient() {
    }

    /**
     * @return 单例对象
     */
    public static LoginClient getInstance() {
        return loginClient;
    }

    /**
     * 设置登录信息
     * 
     * @param url url
     * @param principal 认证用户
     * @param keytab keytab文件
     * @param serverPrincipal server端认证用户，可不填
     * @throws Exception
     */
    public void setConfigure(String url, String principal, String keytab, String serverPrincipal) throws Exception {
        if (StringUtils.isBlank(url) || StringUtils.isBlank(principal) || StringUtils.isBlank(keytab)) {
            return;
        }

        this.url = url;
        this.principal = principal;
        this.keytab = keytab;
        this.usekeytab = true;
    }

    public void setConfigure(String url) throws Exception {
        if (StringUtils.isBlank(url)) {
            return;
        }
        this.url = url;
        this.usekeytab = false;
    }

    /**
     * 登录方法 在已经认证过的集群中，可以不需要keytab
     *
     * @throws Exception
     */
    public void login() throws Exception {
        // 通过账号和密码加密文件认证
        if (usekeytab) {
            AuthClient.getInstance().setConfigure(url, principal, keytab);
            AuthClient.getInstance().login();
        } else {
            AuthClient.getInstance().authenticate(url);
        }

        token = TOKEN_PREFIX + AuthClient.getInstance().getToken();

        if (StringUtils.isBlank(token)) {
            return;
        }

        isLogin = true;
    }

    public String getToken() {
        return token;
    }


    public String getPrincipal() {
        return principal;
    }
}
