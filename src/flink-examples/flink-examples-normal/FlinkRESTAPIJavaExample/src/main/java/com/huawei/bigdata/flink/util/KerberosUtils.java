/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.bigdata.flink.util;

import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * helper class for Java Kerberos setup.
 *
 * @since 2020/10/10
 */
public class KerberosUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KerberosUtils.class);

    private static String clientPrincipal;

    private static String keytabFile;

    private static class KerberosConfiguration extends Configuration {
        private String principal;

        private String keyTab;

        public KerberosConfiguration(String principal, String keyTab) {
            this.principal = principal;
            this.keyTab = keyTab;
        }

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            Map<String, String> options = new HashMap<String, String>();
            options.put("keyTab", keyTab);
            options.put("principal", principal);
            options.put("useKeyTab", "true");
            options.put("storeKey", "false");
            options.put("doNotPrompt", "true");
            options.put("useTicketCache", "false");
            options.put("renewTGT", "false");
            options.put("refreshKrb5Config", "true");
            options.put("isInitiator", "true");
            options.put("debug", "true");

            return new AppConfigurationEntry[] {
                new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options),
            };
        }
    }

    private static <T> T doAs(final Callable<T> callable) throws Exception {
        LoginContext loginContext = null;
        try {
            Set<Principal> principals = new HashSet<Principal>();
            principals.add(new KerberosPrincipal(clientPrincipal));
            Subject subject = new Subject(false, principals, new HashSet<Object>(), new HashSet<Object>());
            loginContext = new LoginContext("", subject, null, new KerberosConfiguration(clientPrincipal, keytabFile));

            loginContext.login();
            subject = loginContext.getSubject();

            return Subject.doAs(subject, new PrivilegedExceptionAction<T>() {
                @Override
                public T run() throws Exception {
                    return callable.call();
                }
            });
        } catch (PrivilegedActionException ex) {
            throw ex.getException();
        } finally {
            try {
                if (loginContext != null) {
                    loginContext.logout();
                }
            } catch (Exception ex) {
                LOG.warn("Failed to logout.", ex);
            }
        }
    }

    /**
     * 认证并回调
     *
     * @param principal 认证用户
     * @param keytab    keytab文件
     * @param callable  回调函数
     * @param <T>       回调函数返回值
     * @return T
     * @throws Exception
     */
    public static <T> T doAsClient(String principal, String keytab, Callable<T> callable) throws Exception {
        clientPrincipal = principal;
        keytabFile = keytab;

        return doAs(callable);
    }
}
