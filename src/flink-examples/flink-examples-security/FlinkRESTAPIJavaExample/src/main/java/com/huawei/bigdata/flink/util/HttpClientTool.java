/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.util;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * 利用HttpClientTool请求
 *
 * @since 2021/10/16
 */
public class HttpClientTool {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpClientTool.class);

    private static final String KRB5_CONF = "java.security.krb5.conf";

    private static final String CHAR_SET = "UTF-8";

    private static final int ERROR = 77;

    private boolean isSecurity = false;

    public int run(String[] args) {
        JcomArgs jcomArgs = new JcomArgs();
        JCommander jc = new JCommander(jcomArgs, args);
        try {
            this.isSecurity = Boolean.valueOf(jcomArgs.security).booleanValue();
            if (jcomArgs.option.equalsIgnoreCase("post")) {
                postClientTool(jcomArgs);
            } else if (jcomArgs.option.equalsIgnoreCase("get")) {
                getClientTool(jcomArgs);
            } else {
                LOGGER.error("UNKNOWN OPERATION.");
                return ERROR;
            }
        } catch (Throwable e) {
            LOGGER.error("HttpClientTool failed.", e);
            return ERROR;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        HttpClientTool tool = new HttpClientTool();
        System.exit(tool.run(args));
    }

    private String requestClientTool(JcomArgs jcomArgs, Supplier<String> supplier) throws Exception {
        if (this.isSecurity) {
            String oldKrb5 = System.getProperty(KRB5_CONF);
            try {
                login(jcomArgs);
                return supplier.get();
            } catch (Exception e) {
                LOGGER.error("Client action error", e);
                throw e;
            } finally {
                if (!Strings.isNullOrEmpty(oldKrb5)) {
                    System.setProperty(KRB5_CONF, oldKrb5);
                }
            }
        }
        try {
            return supplier.get();
        } catch (Exception e) {
            LOGGER.error("Client action error", e);
            throw e;
        }
    }

    private String postClientTool(JcomArgs jcomArgs) throws Exception {
        return requestClientTool(jcomArgs, () -> {
            try {
                return HttpClientUtil.doPost(jcomArgs.url, jcomArgs.jsonStr, CHAR_SET, this.isSecurity);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private String getClientTool(JcomArgs jcomArgs) throws Exception {
        return requestClientTool(jcomArgs, () -> {
            try {
                return HttpClientUtil.doGet(jcomArgs.url, CHAR_SET);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void login(JcomArgs jcomArgs) throws Exception {
        System.setProperty(KRB5_CONF, jcomArgs.krb5Conf);
        if (StringUtils.isNotEmpty(jcomArgs.keytab) && StringUtils.isNotEmpty(jcomArgs.principal)) {
            LoginClient.getInstance().setConfigure(jcomArgs.url, jcomArgs.principal, jcomArgs.keytab, "");
        } else {
            LoginClient.getInstance().setConfigure(jcomArgs.url);
        }
        LoginClient.getInstance().login();
    }

    static class JcomArgs {
        @Parameter(names = {"-o"}, description = "get/post")
        private String option;

        @Parameter(names = "-s", description = "get/post")
        private String security;

        @Parameter(names = "-k", description = "krb5Path")
        private String krb5Conf;

        @Parameter(names = "-u", description = "url")
        private String url;

        @Parameter(names = "-j", description = "post jsonStr")
        private String jsonStr;

        @Parameter(names = "-p", description = "principal")
        private String principal;

        @Parameter(names = "-kt", description = "keytab")
        private String keytab;
    }
}
