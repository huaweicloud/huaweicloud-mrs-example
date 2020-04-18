package com.huawei.hadoop.security;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.log4j.Logger;

public class LoginUtil {
    public enum Module {
        STORM("StormClient"), KAFKA("KafkaClient"), ZOOKEEPER("Client");

        private String name;

        private Module(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static final Logger LOG = Logger.getLogger(LoginUtil.class);

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
    public static final String JAVA_SECURITY_LOGIN_CONF_KEY = "java.security.auth.login.config";

    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";
    
    private static final String ZOOKEEPER_SASL_CLIENT = "zookeeper.sasl.client";

    private static final String LOGIN_FAILED_CAUSE_PASSWORD_WRONG =
            "(wrong password) keytab file and user not match, you can kinit -k -t keytab user in client server to check";

    private static final String LOGIN_FAILED_CAUSE_TIME_WRONG =
            "(clock skew) time of local server and remote server not match, please check ntp to remote server";

    private static final String LOGIN_FAILED_CAUSE_AES256_WRONG =
            "(aes256 not support) aes256 not support by default jdk/jre, need copy local_policy.jar and US_export_policy.jar from remote server in path /opt/huawei/Bigdata/jdk/jre/lib/security";

    private static final String LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG =
            "(no rule) principal format not support by default, need add property hadoop.security.auth_to_local(in core-site.xml) value RULE:[1:$1] RULE:[2:$1]";

    private static final String LOGIN_FAILED_CAUSE_TIME_OUT =
            "(time out) can not connect to kdc server or there is fire wall in the network";

    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");
  
    public static void setKrb5Config(String krb5ConfFile)
            throws IOException {
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

    public static void setJaasFile(String principal, String keytabPath)
            throws IOException {
        String jaasPath =
                new File(System.getProperty("java.io.tmpdir")) + File.separator + System.getProperty("user.name")
                        + JAAS_POSTFIX;

        // windows路径下分隔符替换
        jaasPath = jaasPath.replace("\\", "\\\\");
        keytabPath = keytabPath.replace("\\", "\\\\");
        // 删除jaas文件
        deleteJaasFile(jaasPath);
        writeJaasFile(jaasPath, principal, keytabPath);
        System.setProperty(JAVA_SECURITY_LOGIN_CONF_KEY, jaasPath);
    }

    private static void writeJaasFile(String jaasPath, String principal, String keytabPath)
            throws IOException {
        FileWriter writer = new FileWriter(new File(jaasPath));
        try {
            writer.write(getJaasConfContext(principal, keytabPath));
            writer.flush();
        } catch (IOException e) {
            throw new IOException("Failed to create jaas.conf File");
        } finally {
            writer.close();
        }
    }

    private static void deleteJaasFile(String jaasPath)
            throws IOException {
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
            builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
            builder.append("useKeytab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
            builder.append("debug=true;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        } else {
            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("useKeyTab=true").append(LINE_SEPARATOR);
            builder.append("keyTab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
            builder.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
            builder.append("useTicketCache=false").append(LINE_SEPARATOR);
            builder.append("storeKey=true").append(LINE_SEPARATOR);
            builder.append("debug=true;").append(LINE_SEPARATOR);
            builder.append("};").append(LINE_SEPARATOR);
        }

        return builder.toString();
    }

    public static void setJaasConf(String loginContextName, String principal, String keytabFile)
            throws IOException {
        if ((loginContextName == null) || (loginContextName.length() <= 0)) {
            LOG.error("input loginContextName is invalid.");
            throw new IOException("input loginContextName is invalid.");
        }

        if ((principal == null) || (principal.length() <= 0)) {
            LOG.error("input principal is invalid.");
            throw new IOException("input principal is invalid.");
        }

        if ((keytabFile == null) || (keytabFile.length() <= 0)) {
            LOG.error("input keytabFile is invalid.");
            throw new IOException("input keytabFile is invalid.");
        }

        File userKeytabFile = new File(keytabFile);
        if (!userKeytabFile.exists()) {
            LOG.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
        }
        String keyTabFilePath = userKeytabFile.getAbsolutePath();
        javax.security.auth.login.Configuration.setConfiguration(new JaasConfiguration(loginContextName, principal,
                keyTabFilePath));

        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof JaasConfiguration)) {
            LOG.error("javax.security.auth.login.Configuration is not JaasConfiguration.");
            throw new IOException("javax.security.auth.login.Configuration is not JaasConfiguration.");
        }

        AppConfigurationEntry[] entrys = conf.getAppConfigurationEntry(loginContextName);
        if (entrys == null) {
            LOG.error("javax.security.auth.login.Configuration has no AppConfigurationEntry named " + loginContextName
                    + ".");
            throw new IOException("javax.security.auth.login.Configuration has no AppConfigurationEntry named "
                    + loginContextName + ".");
        }

        boolean checkPrincipal = false;
        boolean checkKeytab = false;
        for (int i = 0; i < entrys.length; i++) {
            if (entrys[i].getOptions().get("principal").equals(principal)) {
                checkPrincipal = true;
            }

            if (IS_IBM_JDK) {
                if (entrys[i].getOptions().get("useKeytab").equals(keytabFile)) {
                    checkKeytab = true;
                }
            } else {
                if (entrys[i].getOptions().get("keyTab").equals(keyTabFilePath)) {
                    checkKeytab = true;
                }
            }

        }

        if (!checkPrincipal) {
            LOG.error("AppConfigurationEntry named " + loginContextName + " does not have principal value of "
                    + principal + ".");
            throw new IOException("AppConfigurationEntry named " + loginContextName
                    + " does not have principal value of " + principal + ".");
        }

        if (!checkKeytab) {
            LOG.error("AppConfigurationEntry named " + loginContextName + " does not have keyTab value of "
                    + keytabFile + ".");
            throw new IOException("AppConfigurationEntry named " + loginContextName + " does not have keyTab value of "
                    + keytabFile + ".");
        }

    }

    public static void setZookeeperServerPrincipal(String zkServerPrincipal)
            throws IOException {
        System.setProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY, zkServerPrincipal);
        String ret = System.getProperty(ZOOKEEPER_SERVER_PRINCIPAL_KEY);
        if (ret == null) {
            LOG.error(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is null.");
            throw new IOException(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is null.");
        }
        if (!ret.equals(zkServerPrincipal)) {
            LOG.error(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is " + ret + " is not " + zkServerPrincipal
                    + ".");
            throw new IOException(ZOOKEEPER_SERVER_PRINCIPAL_KEY + " is " + ret + " is not "
                    + zkServerPrincipal + ".");
        }
    }
    
    public static void setSaslClient(String isSaslClient)
            throws IOException {
        System.setProperty(ZOOKEEPER_SASL_CLIENT, isSaslClient);
        String ret = System.getProperty(ZOOKEEPER_SASL_CLIENT);
        if (ret == null) {
            LOG.error(ZOOKEEPER_SASL_CLIENT + " is null.");
            throw new IOException(ZOOKEEPER_SASL_CLIENT + " is null.");
        }
        if (!ret.equals(isSaslClient)) {
            LOG.error(ZOOKEEPER_SASL_CLIENT + " is " + ret + " is not " + isSaslClient
                + ".");
            throw new IOException(ZOOKEEPER_SASL_CLIENT + " is " + ret + " is not "
                + isSaslClient + ".");
        }
    }

    /**
     * copy from hbase zkutil 0.94&0.98 A JAAS configuration that defines the login modules that we want to use for
     * login.
     */
    private static class JaasConfiguration extends javax.security.auth.login.Configuration {
        private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<String, String>();

        static {
            String jaasEnvVar = System.getenv("HBASE_JAAS_DEBUG");
            if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
                BASIC_JAAS_OPTIONS.put("debug", "true");
            }
        }

        private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap<String, String>();

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

        private javax.security.auth.login.Configuration baseConfig;

        private final String loginContextName;

        private final boolean useTicketCache;

        private final String keytabFile;

        private final String principal;


        public JaasConfiguration(String loginContextName, String principal, String keytabFile) throws IOException {
            this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.length() == 0);
        }

        private JaasConfiguration(String loginContextName, String principal, String keytabFile, boolean useTicketCache) throws IOException {
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
            LOG.info("JaasConfiguration loginContextName=" + loginContextName + " principal=" + principal
                    + " useTicketCache=" + useTicketCache + " keytabFile=" + keytabFile);
        }

        private void initKerberosOption() throws IOException {
            if (!useTicketCache) {
                if (IS_IBM_JDK) {
                    KEYTAB_KERBEROS_OPTIONS.put("useKeytab", keytabFile);
                } else {
                    KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
                    KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
                    KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", useTicketCache ? "true" : "false");
                }
            }
            KEYTAB_KERBEROS_OPTIONS.put("principal", principal);
        }

        public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
            if (baseConfig != null)
                return baseConfig.getAppConfigurationEntry(appName);
            return (null);
        }
    }
}
