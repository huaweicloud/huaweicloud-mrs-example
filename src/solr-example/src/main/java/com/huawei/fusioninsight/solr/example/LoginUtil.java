/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2019-2020. All rights reserved.
 */

package com.huawei.fusioninsight.solr.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

/**
 * 安全模式下登录集群的工具类
 *
 * @since 2020-03-04
 */
public class LoginUtil {
    /**
     * jaas.conf中的client模块名称
     */
    public enum Module {
        STORM("StormClient"),
        KAFKA("KafkaClient"),
        SOLR("SolrClient"),
        ZOOKEEPER("Client");

        private String name;

        Module(String name) {
            this.name = name;
        }

        /**
         * 获取模块名称
         *
         * @return name
         */
        public String getName() {
            return name;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(LoginUtil.class);

    /**
     * line operator string
     */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    /**
     * jaas file postfix
     */
    private static final String JAAS_POSTFIX = ".jaas.conf";

    /**
     * IBM jdk login module
     */
    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    /**
     * oracle jdk login module
     */
    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    /**
     * java security login file path
     */
    private static final String JAVA_SECURITY_LOGIN_CONF_KEY = "java.security.auth.login.config";

    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String LOGIN_FAILED_CAUSE_PASSWORD_WRONG =
            "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to"
                + " check";

    private static final String LOGIN_FAILED_CAUSE_TIME_WRONG =
            "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    private static final String LOGIN_FAILED_CAUSE_AES256_WRONG =
            "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and"
                + " US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    private static final String LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG =
            "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in"
                + " core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    private static final String LOGIN_FAILED_CAUSE_TIME_OUT =
            "(time out) can not connect to kdc server or there is fire wall in the network";

    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    /**
     * 安全模式下登录系统
     *
     * @param userPrincipal  登录用户的principal
     * @param userKeytabPath 登录用户的keytab文件
     * @param krb5ConfPath   登录用户的krb5文件
     * @param conf           配置集
     * @throws IOException 异常
     */
    public static synchronized void login(String userPrincipal, String userKeytabPath, String krb5ConfPath,
        Configuration conf) throws IOException {
        // 1.check input parameters
        if (!checkParams(userPrincipal)) {
            throwParamInputError(userPrincipal);
        }
        if (!checkParams(userKeytabPath)) {
            throwParamInputError(userKeytabPath);
        }
        if (!checkParams(krb5ConfPath)) {
            throwParamInputError(krb5ConfPath);
        }
        if ((conf == null)) {
            throwParamInputError("conf");
        }
        // 2.check file exsits
        File userKeytabFile = new File(userKeytabPath);
        if (!userKeytabFile.exists()) {
            throwFileNotExistError(userKeytabFile);
        }
        if (!userKeytabFile.isFile()) {
            throwIsNotFileError(userKeytabFile);
        }

        File krb5ConfFile = new File(krb5ConfPath);
        if (!krb5ConfFile.exists()) {
            throwFileNotExistError(krb5ConfFile);
        }
        if (!krb5ConfFile.isFile()) {
            throwIsNotFileError(krb5ConfFile);
        }

        // 3.set and check krb5config
        setKrb5Config(krb5ConfFile.getCanonicalPath());
        setConfiguration(conf);

        // 4.login and check for hadoop
        loginHadoop(userPrincipal, userKeytabFile.getCanonicalPath());
        LOG.info("Login success.");
    }

    private static void setConfiguration(Configuration conf) {
        UserGroupInformation.setConfiguration(conf);
    }

    private static boolean checkNeedLogin(String principal) throws IOException {
        if (!UserGroupInformation.isSecurityEnabled()) {
            LOG.error(
                "UserGroupInformation is not SecurityEnabled, please check if core-site.xml exists in classpath.");
            throw new IOException(
                "UserGroupInformation is not SecurityEnabled, please check if core-site.xml exists in classpath.");
        }
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if ((currentUser != null) && (currentUser.hasKerberosCredentials())) {
            if (checkCurrentUserCorrect(principal)) {
                LOG.info("current user is {} has login.", currentUser);
                if (!currentUser.isFromKeytab()) {
                    LOG.error("current user is not from keytab.");
                    throw new IOException("current user is not from keytab.");
                }
                return false;
            } else {
                LOG.error(
                    String.format(Locale.ENGLISH, "current user is {} has login. please check your environment, %s",
                        "especially when it used IBM JDK or kerberos for OS count login."), currentUser);
                throw new IOException(
                    String.format(Locale.ENGLISH, "current user is %s has login. And please check your environment.",
                        currentUser));
            }
        }
        return true;
    }

    /**
     * 设置安全模式下使用的krb5配置文件
     *
     * @param krb5ConfFile 带全路径的krb5文件
     * @throws IOException 异常
     */
    public static void setKrb5Config(String krb5ConfFile) throws IOException {
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null) {
            LOG.error("The {} is null.", JAVA_SECURITY_KRB5_CONF_KEY);
            throw new IOException(String.format(Locale.ENGLISH, "The %s value is null.", JAVA_SECURITY_KRB5_CONF_KEY));
        }
        if (!ret.equals(krb5ConfFile)) {
            LOG.error("The {} value is {}, not equal {}.", JAVA_SECURITY_KRB5_CONF_KEY, ret, krb5ConfFile);
            throw new IOException(
                String.format(Locale.ENGLISH, "The %s value is %s, not equal %s.", JAVA_SECURITY_KRB5_CONF_KEY, ret,
                    krb5ConfFile));
        }
    }

    /**
     * 设置安全模式下使用的jaas配置文件
     *
     * @param principal  登录用户的principal
     * @param keytab 带全路径keytab文件
     * @throws IOException 异常
     */
    public static void setJaasFile(String principal, String keytab) throws IOException {
        String keytabPath = keytab;
        String jaasPath = new StringBuffer().append(new File(System.getProperty("java.io.tmpdir")))
            .append(File.separator)
            .append(System.getProperty("user.name"))
            .append(JAAS_POSTFIX)
            .toString();

        // windows路径下分隔符替换
        jaasPath = jaasPath.replace("\\", "\\\\");
        keytabPath = keytabPath.replace("\\", "\\\\");
        // 删除jaas文件
        deleteJaasFile(jaasPath);
        writeJaasFile(jaasPath, principal, keytabPath);
        System.setProperty(JAVA_SECURITY_LOGIN_CONF_KEY, jaasPath);
    }

    private static void writeJaasFile(String jaasPath, String principal, String keytabPath) throws IOException {
        try (FileWriter writer = new FileWriter(new File(jaasPath))) {
            writer.write(getJaasConfContext(principal, keytabPath));
            writer.flush();
        } catch (IOException e) {
            throw new IOException("Failed to create jaas.conf File");
        }
    }

    private static void deleteJaasFile(String jaasPath) throws IOException {
        File jaasFile = new File(jaasPath);
        if (jaasFile.exists()) {
            if (!jaasFile.delete()) {
                throw new IOException("Failed to delete exists jaas file.");
            }
        }
    }

    private static String getJaasConfContext(String principal, String keytabPath) {
        Module[] allModule = Module.values();
        StringBuilder builder = new StringBuilder();
        for (Module modlue : allModule) {
            builder.append(getModuleContext(principal, keytabPath, modlue));
        }
        return builder.toString();
    }

    private static String getModuleContext(String userPrincipal, String keyTabPath, Module module) {
        StringBuilder builder = new StringBuilder();
        if (IS_IBM_JDK) {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(IBM_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("credsType=both").append(LINE_SEPARATOR);
            builder.append("principal=\"").append(userPrincipal).append("\"").append(LINE_SEPARATOR);
            builder.append("useKeytab=\"").append(keyTabPath).append("\"").append(LINE_SEPARATOR);
            builder.append("debug=true;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        } else {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("useKeyTab=true").append(LINE_SEPARATOR);
            builder.append("keyTab=\"").append(keyTabPath).append("\"").append(LINE_SEPARATOR);
            builder.append("principal=\"").append(userPrincipal).append("\"").append(LINE_SEPARATOR);
            builder.append("useTicketCache=false").append(LINE_SEPARATOR);
            builder.append("storeKey=true").append(LINE_SEPARATOR);
            builder.append("debug=true;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        }

        return builder.toString();
    }

    /**
     * 设置jaas配置
     *
     * @param loginContextName 登录名称
     * @param principal        登录用户principal
     * @param keytabFile       keytab文件
     * @throws IOException 异常
     */
    public static void setJaasConf(String loginContextName, String principal, String keytabFile) throws IOException {
        if (!checkParams(loginContextName)) {
            throwParamInputError(loginContextName);
        }

        if (!checkParams(principal)) {
            throwParamInputError(principal);
        }

        if (!checkParams(keytabFile)) {
            throwParamInputError(keytabFile);
        }

        File userKeytabFile = new File(keytabFile);
        if (!userKeytabFile.exists()) {
            throwFileNotExistError(userKeytabFile);
        }

        javax.security.auth.login.Configuration.setConfiguration(
            new JaasConfiguration(loginContextName, principal, userKeytabFile.getCanonicalPath()));
        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof JaasConfiguration)) {
            LOG.error("javax.security.auth.login.Configuration is not JaasConfiguration.");
            throw new IOException("javax.security.auth.login.Configuration is not JaasConfiguration.");
        }

        AppConfigurationEntry[] entryArray = conf.getAppConfigurationEntry(loginContextName);
        if (entryArray == null) {
            LOG.error("javax.security.auth.login.Configuration has no AppConfigurationEntry named {}.",
                loginContextName);
            throw new IOException(String.format(Locale.ENGLISH,
                "javax.security.auth.login.Configuration has no AppConfigurationEntry named %s.", loginContextName));
        }

        checkAPPConfigurationEntry(loginContextName, principal, keytabFile, entryArray);
    }

    private static void checkAPPConfigurationEntry(String loginContextName, String principal, String keytabFile,
        AppConfigurationEntry[] entryArray) throws IOException {
        boolean checkPrincipal = false;
        boolean checkKeytab = false;
        for (AppConfigurationEntry entry : entryArray) {
            if (entry.getOptions().get("principal").equals(principal)) {
                checkPrincipal = true;
            }

            if (IS_IBM_JDK) {
                if (entry.getOptions().get("useKeytab").equals(keytabFile)) {
                    checkKeytab = true;
                }
            } else {
                if (entry.getOptions().get("keyTab").equals(keytabFile)) {
                    checkKeytab = true;
                }
            }
        }

        if (!checkPrincipal) {
            LOG.error("AppConfigurationEntry named {} does not have principal value of {}.", loginContextName,
                principal);
            throw new IOException(
                String.format(Locale.ENGLISH, "AppConfigurationEntry named %s does not have principal value of %s.",
                    loginContextName, principal));
        }

        if (!checkKeytab) {
            LOG.error("AppConfigurationEntry named {} does not have keyTab value of {}.", loginContextName, keytabFile);
            throw new IOException(
                String.format(Locale.ENGLISH, "AppConfigurationEntry named %s does not have keyTab value of %s.",
                    loginContextName, keytabFile));
        }
    }

    /**
     * 设置zookeeper服务的principal名称
     *
     * @param zkServerPrincipal zk服务的principal名称
     * @throws IOException 异常
     */
    public static void setZookeeperServerPrincipal(String zkServerPrincipal) throws IOException {
        System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, zkServerPrincipal);
        String ret = System.getProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY);
        if (ret == null) {
            LOG.error("The {} is null.", ZOOKEEPER_SERVER_PRINCIPAL_KEY);
            throw new IOException(String.format(Locale.ENGLISH, "The %s is null.", ZOOKEEPER_SERVER_PRINCIPAL_KEY));
        }
        if (!ret.equals(zkServerPrincipal)) {
            LOG.error("The {} is {}, is not equal {}.", ZOOKEEPER_SERVER_PRINCIPAL_KEY, ret, zkServerPrincipal);
            throw new IOException(
                String.format(Locale.ENGLISH, "The %s is %s, is not equal %s.", ZOOKEEPER_SERVER_PRINCIPAL_KEY, ret,
                    zkServerPrincipal));
        }
    }

    /**
     * 设置zookeeper服务用户
     */
    @Deprecated
    public static void setZookeeperServerPrincipal(String zkServerPrincipalKey, String zkServerPrincipal)
        throws IOException {
        System.setProperty(zkServerPrincipalKey, zkServerPrincipal);
        String ret = System.getProperty(zkServerPrincipalKey);
        if (ret == null) {
            LOG.error("The {} is null.", zkServerPrincipalKey);
            throw new IOException(String.format(Locale.ENGLISH, "The %s is null.", zkServerPrincipalKey));
        }
        if (!ret.equals(zkServerPrincipal)) {
            LOG.error("The {} is {}, is not equal {}.", zkServerPrincipalKey, ret, zkServerPrincipal);
            throw new IOException(
                String.format(Locale.ENGLISH, "The %s is %s, is not equal %s.", zkServerPrincipalKey, ret,
                    zkServerPrincipal));
        }
    }

    private static void loginHadoop(String principal, String keytabFile) throws IOException {
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        } catch (IOException e) {
            LOG.error("login failed with {} and {}.", principal, keytabFile);
            LOG.error("perhaps cause 1 is {}.", LOGIN_FAILED_CAUSE_PASSWORD_WRONG);
            LOG.error("perhaps cause 2 is {}.", LOGIN_FAILED_CAUSE_TIME_WRONG);
            LOG.error("perhaps cause 3 is {}.", LOGIN_FAILED_CAUSE_AES256_WRONG);
            LOG.error("perhaps cause 4 is {}.", LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG);
            LOG.error("perhaps cause 5 is {}.", LOGIN_FAILED_CAUSE_TIME_OUT);

            throw e;
        }
    }

    private static void checkAuthenticateOverKrb() throws IOException {
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if (loginUser == null) {
            LOG.error("The current user is {}, but loginUser is null.", currentUser);
            throw new IOException(
                String.format(Locale.ENGLISH, "The current user is %s, but loginUser is null.", currentUser));
        }
        if (!loginUser.equals(currentUser)) {
            LOG.error("The current user is {}, but loginUser is {}.", currentUser, loginUser);
            throw new IOException(
                String.format(Locale.ENGLISH, "The current user is %s, but loginUser is %s.", currentUser, loginUser));
        }
        if (!loginUser.hasKerberosCredentials()) {
            LOG.error("The current user is {} has no Kerberos Credentials.", currentUser);
            throw new IOException(
                String.format(Locale.ENGLISH, "The current user is %s has no Kerberos Credentials.", currentUser));
        }
        if (!UserGroupInformation.isLoginKeytabBased()) {
            LOG.error("The current user is {} is not Login Keytab Based.", currentUser);
            throw new IOException(
                String.format(Locale.ENGLISH, "The current user is %s is not Login Keytab Based.", currentUser));
        }
    }

    private static boolean checkCurrentUserCorrect(String principal) throws IOException {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        if (ugi == null) {
            LOG.error("current user still null.");
            throw new IOException("current user still null.");
        }

        String defaultRealm;
        try {
            defaultRealm = KerberosUtil.getDefaultRealm();
        } catch (ClassNotFoundException | NoSuchMethodException
            | IllegalAccessException | InvocationTargetException e) {
            LOG.warn("getDefaultRealm failed.");
            throw new IOException(e);
        }

        if ((defaultRealm != null) && (defaultRealm.length() > 0)) {
            StringBuilder realm = new StringBuilder();
            StringBuilder principalWithRealm = new StringBuilder();
            realm.append("@").append(defaultRealm);
            if (!principal.endsWith(realm.toString())) {
                principalWithRealm.append(principal).append(realm);
                principal = principalWithRealm.toString();
            }
        }

        return principal.equals(ugi.getUserName());
    }

    private static boolean checkParams(String param) {
        return (param != null) && (param.length() > 0);
    }

    private static void throwParamInputError(String param) throws IOException {
        LOG.error("The input {} is invalid.", param);
        throw new IOException(String.format(Locale.ENGLISH, "The input %s is invalid.", param));
    }

    private static void throwFileNotExistError(File filename) throws IOException {
        LOG.error("The filename {}({}) does not exist.", filename, filename.getCanonicalPath());
        throw new IOException(String.format(Locale.ENGLISH, "The filename %s(%s) does not exist.", filename,
            filename.getCanonicalPath()));
    }

    private static void throwIsNotFileError(File filename) throws IOException {
        LOG.error("The filename {}({}) is not a file.", filename, filename.getCanonicalPath());
        throw new IOException(
            String.format(Locale.ENGLISH, "The filename %s(%s) is not a file.", filename, filename.getCanonicalPath()));
    }

    /**
     * copy from hbase zkutil 0.94&0.98 A JAAS configuration that defines the login modules that we want to use for
     * login.
     */
    private static class JaasConfiguration extends javax.security.auth.login.Configuration {
        private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<>();

        static {
            String jaasEnvVar = System.getenv("HBASE_JAAS_DEBUG");
            if ("true".equalsIgnoreCase(jaasEnvVar)) {
                BASIC_JAAS_OPTIONS.put("debug", "true");
            }
        }

        private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap<>();

        static {
            if (IS_IBM_JDK) {
                KEYTAB_KERBEROS_OPTIONS.put("credsType", "both");
            } else {
                KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
                KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", "false");
                KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
                KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
            }

            KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
        }

        private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN =
                new AppConfigurationEntry(
                        KerberosUtil.getKrb5LoginModuleName(),
                        LoginModuleControlFlag.REQUIRED,
                        KEYTAB_KERBEROS_OPTIONS);

        private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF =
                new AppConfigurationEntry[] {KEYTAB_KERBEROS_LOGIN};

        private javax.security.auth.login.Configuration baseConfig;

        private final String loginContextName;

        private final boolean useTicketCache;

        private final String keytabFile;

        private final String principal;

        public JaasConfiguration(String loginContextName, String principal, String keytabFile) {
            this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.length() == 0);
        }

        private JaasConfiguration(String loginContextName, String principal, String keytabFile,
            boolean useTicketCache) {
            try {
                this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
            } catch (SecurityException e) {
                this.baseConfig = null;
            }
            this.loginContextName = loginContextName;
            this.useTicketCache = useTicketCache;
            this.keytabFile = keytabFile;
            this.principal = principal;

            initKerberosOption();
            LOG.info("JaasConfiguration loginContextName={}, principal={}, useTicketCache={}, keytabFile={}.",
                loginContextName, principal, useTicketCache, keytabFile);
        }

        private void initKerberosOption() {
            if (!useTicketCache) {
                if (IS_IBM_JDK) {
                    KEYTAB_KERBEROS_OPTIONS.put("useKeytab", keytabFile);
                } else {
                    KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
                    KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
                    KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", "false");
                }
            }
            KEYTAB_KERBEROS_OPTIONS.put("principal", principal);
        }

        /**
         * 获取应用配置集合
         *
         * @param appName 应用名称
         * @return 配置集合
         */
        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
            if (loginContextName.equals(appName)) {
                return KEYTAB_KERBEROS_CONF;
            }
            if (baseConfig != null) {
                return baseConfig.getAppConfigurationEntry(appName);
            }
            return (null);
        }
    }
}
