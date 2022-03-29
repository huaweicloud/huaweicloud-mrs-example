/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.bigdata.spark.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

/**
 * Login Util for Client of Kafka, Elasticsearch and ZooKeeper.
 */
public class LoginUtil {
    /**
     * The module of each Client
     */
    public enum Module {
        KAFKA("KafkaClient"),
        ELASTICSEARCH("EsClient"),
        ZOOKEEPER("Client");

        private final String name;

        Module(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(LoginUtil.class);

    /**
     * line operator string
     */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String ES = "es.";

    /**
     * jaas file postfix
     */
    private static final String JAAS_POSTFIX = ".jaas.conf";

    /**
     * IBM jdk login module
     */
    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    /**
     * Oracle jdk login module
     */
    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    /**
     * java security login file path
     */
    private static final String JAVA_SECURITY_LOGIN_CONF_KEY = "java.security.auth.login.config";

    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

    private static final String LOGIN_FAILED_CAUSE_PASSWORD_WRONG =
            "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to "
                    + "check";

    private static final String LOGIN_FAILED_CAUSE_TIME_WRONG =
            "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    private static final String LOGIN_FAILED_CAUSE_AES256_WRONG =
            "(AES256 not support) AES256 not support by default JDK/JRE, need copy local_policy.jar and "
                    + "US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    private static final String LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG =
            "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in"
                    + " core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    private static final String LOGIN_FAILED_CAUSE_TIME_OUT =
            "(time out) can not connect to kdc server or there is fire wall in the network";

    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    private static boolean writeFlag = false;

    /**
     * Login method.
     *
     * @apiNote Need to check each param
     */
    public static synchronized void login(
            String userPrincipal, String userKeytabPath, String krb5ConfPath, Configuration conf) throws IOException {
        // 1.check input parameters
        checkParams(userPrincipal, userKeytabPath, krb5ConfPath);
        if ((conf == null)) {
            throwParamInputError("conf");
        }

        // 2.check file exists
        File userKeytabFile = new File(userKeytabPath);
        File krb5ConfFile = new File(krb5ConfPath);
        checkFiles(userKeytabFile, krb5ConfFile);

        // 3.set and check krb5config
        setKrb5Config(krb5ConfFile.getAbsolutePath());
        setConfiguration(conf);

        // 4.login and check for hadoop
        loginHadoop(userPrincipal, userKeytabFile.getAbsolutePath());
        LOG.info("Login success!!!!!!!!!!!!!!");
    }

    private static void checkParams(String... params) throws IOException {
        for (String param : params) {
            if (isParamInvalid(param)) {
                throwParamInputError(param);
            }
        }
    }

    private static void checkFiles(File... files) throws IOException {
        for (File file : files) {
            if (!file.exists()) {
                throwFileNotExistError(file);
            }
            if (!file.isFile()) {
                throwIsNotFileError(file);
            }
        }
    }

    private static void setConfiguration(Configuration conf) throws IOException {
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
                LOG.info("Current user is {} has login.", currentUser);
                if (!currentUser.isFromKeytab()) {
                    LOG.error("Current user is not from keytab.");
                    throw new IOException("Current user is not from keytab.");
                }
                return false;
            } else {
                LOG.error(
                        "Current user is "
                                + currentUser
                                + " has login. Please check your environment, especially when it used IBM JDK or"
                                + " kerberos for OS count login!");
                throw new IOException("Current user is " + currentUser + " has login. Please check your environment!");
            }
        }

        return true;
    }

    /**
     * Set krb5 file into runtime env.
     */
    public static void setKrb5Config(String krb5ConfFile) throws IOException {
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null) {
            LOG.error(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
        }
        if (!ret.equals(krb5ConfFile)) {
            LOG.error(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
        }
    }

    /**
     * Set jaas file into runtime env.
     */
    public static synchronized void setJaasFile(String principal, String keytabPath) throws IOException {
        String filePath = keytabPath.substring(0, keytabPath.lastIndexOf(File.separator));
        String jaasPath = filePath + File.separator + ES + System.getProperty("user.name") + JAAS_POSTFIX;

        // Windows路径下分隔符替换
        jaasPath = jaasPath.replace("\\", "\\\\");
        String tmpKeytabPath = keytabPath.replace("\\", "\\\\");
        LOG.info("jaasPath is {}", jaasPath);
        LOG.info("keytabPath is {}", tmpKeytabPath);
        // 删除jaas文件
        if (new File(jaasPath).exists()) {
            if (!writeFlag) {
                deleteJaasFile(jaasPath);
                writeJaasFile(jaasPath, principal, tmpKeytabPath);
                System.setProperty(JAVA_SECURITY_LOGIN_CONF_KEY, jaasPath);
                writeFlag = true;
            }
        } else {
            writeJaasFile(jaasPath, principal, tmpKeytabPath);
            System.setProperty(JAVA_SECURITY_LOGIN_CONF_KEY, jaasPath);
            writeFlag = true;
        }
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
        for (Module module : allModule) {
            builder.append(getModuleContext(principal, keytabPath, module));
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
     * Set jaas file into runtime env.
     */
    public static void setJaasConf(String loginContextName, String principal, String keytabFile) throws IOException {
        checkParams(loginContextName, principal, keytabFile);

        File userKeytabFile = new File(keytabFile);
        if (!userKeytabFile.exists()) {
            throwFileNotExistError(userKeytabFile);
        }

        javax.security.auth.login.Configuration.setConfiguration(
                new JaasConfiguration(loginContextName, principal, userKeytabFile.getAbsolutePath()));
        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof JaasConfiguration)) {
            LOG.error("javax.security.auth.login.Configuration is not JaasConfiguration.");
            throw new IOException("javax.security.auth.login.Configuration is not JaasConfiguration.");
        }

        AppConfigurationEntry[] entries = conf.getAppConfigurationEntry(loginContextName);
        if (entries == null) {
            LOG.error(
                    "javax.security.auth.login.Configuration has no AppConfigurationEntry named "
                            + loginContextName
                            + ".");
            throw new IOException(
                    "javax.security.auth.login.Configuration has no AppConfigurationEntry named "
                            + loginContextName
                            + ".");
        }

        boolean checkPrincipal = false;
        boolean checkKeytab = false;
        for (AppConfigurationEntry entry : entries) {
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
            LOG.error(
                    "AppConfigurationEntry named "
                            + loginContextName
                            + " does not have principal value of "
                            + principal
                            + ".");
            throw new IOException(
                    "AppConfigurationEntry named "
                            + loginContextName
                            + " does not have principal value of "
                            + principal
                            + ".");
        }

        if (!checkKeytab) {
            LOG.error(
                    "AppConfigurationEntry named "
                            + loginContextName
                            + " does not have keyTab value of "
                            + keytabFile
                            + ".");
            throw new IOException(
                    "AppConfigurationEntry named "
                            + loginContextName
                            + " does not have keyTab value of "
                            + keytabFile
                            + ".");
        }
    }

    public static void setZookeeperServerPrincipal(String zkServerPrincipal) throws IOException {
        System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, zkServerPrincipal);
        String ret = System.getProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY);

        if (ret == null) {
            LOG.error(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is null.");
            throw new IOException(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is null.");
        }
        if (!ret.equals(zkServerPrincipal)) {
            LOG.error(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is " + ret + " is not " + zkServerPrincipal + ".");
            throw new IOException(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is " + ret + " is not " + zkServerPrincipal + ".");
        }
    }

    private static void loginHadoop(String principal, String keytabFile) throws IOException {
        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        } catch (IOException e) {
            LOG.error("login failed with " + principal + " and " + keytabFile + ".");
            LOG.error("perhaps cause 1 is " + LOGIN_FAILED_CAUSE_PASSWORD_WRONG + ".");
            LOG.error("perhaps cause 2 is " + LOGIN_FAILED_CAUSE_TIME_WRONG + ".");
            LOG.error("perhaps cause 3 is " + LOGIN_FAILED_CAUSE_AES256_WRONG + ".");
            LOG.error("perhaps cause 4 is " + LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG + ".");
            LOG.error("perhaps cause 5 is " + LOGIN_FAILED_CAUSE_TIME_OUT + ".");

            throw e;
        }
    }

    private static void checkAuthenticateOverKrb() throws IOException {
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if (loginUser == null) {
            LOG.error("current user is " + currentUser + ", but loginUser is null.");
            throw new IOException("current user is " + currentUser + ", but loginUser is null.");
        }
        if (!loginUser.equals(currentUser)) {
            LOG.error("current user is " + currentUser + ", but loginUser is " + loginUser + ".");
            throw new IOException("current user is " + currentUser + ", but loginUser is " + loginUser + ".");
        }
        if (!loginUser.hasKerberosCredentials()) {
            LOG.error("current user is " + currentUser + " has no Kerberos Credentials.");
            throw new IOException("current user is " + currentUser + " has no Kerberos Credentials.");
        }
        if (!UserGroupInformation.isLoginKeytabBased()) {
            LOG.error("current user is " + currentUser + " is not Login Keytab Based.");
            throw new IOException("current user is " + currentUser + " is not Login Keytab Based.");
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
        } catch (Exception e) {
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

    private static boolean isParamInvalid(String param) {
        return (param == null) || (param.length() <= 0);
    }

    private static void throwParamInputError(String param) throws IOException {
        LOG.error("input " + param + " is invalid.");
        throw new IOException("input " + param + " is invalid.");
    }

    private static void throwFileNotExistError(File filename) throws IOException {
        LOG.error(filename + "(" + filename.getAbsolutePath() + ") does not exist.");
        throw new IOException(filename + "(" + filename.getAbsolutePath() + ") does not exist.");
    }

    private static void throwIsNotFileError(File filename) throws IOException {
        LOG.error(filename + "(" + filename.getAbsolutePath() + ") is not a file.");
        throw new IOException(filename + "(" + filename.getAbsolutePath() + ") is not a file.");
    }

    /**
     * A JAAS configuration that defines the login modules that we want to use for login.
     */
    private static class JaasConfiguration extends javax.security.auth.login.Configuration {
        private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<>();

        static {
            String jaasEnvVar = System.getenv("ES_JAAS_DEBUG");
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

        public JaasConfiguration(String loginContextName, String principal, String keytabFile) throws IOException {
            this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.length() == 0);
        }

        private JaasConfiguration(String loginContextName, String principal, String keytabFile, boolean useTicketCache)
                throws IOException {
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
            LOG.info(
                    "JaasConfiguration loginContextName={}, principal={}, useTicketCache={}, keytabFile={}",
                    loginContextName,
                    principal,
                    useTicketCache,
                    keytabFile);
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
