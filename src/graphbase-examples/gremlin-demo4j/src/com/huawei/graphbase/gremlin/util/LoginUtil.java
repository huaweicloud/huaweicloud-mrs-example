package com.huawei.graphbase.gremlin.util;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;

public class LoginUtil {
    private static final Logger LOG = LoggerFactory.getLogger(LoginUtil.class);

    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");

    public static void setJaasConf(String loginContextName, String principal, String keytabFile) throws IOException {
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

        File userKeytabFile = FileUtils.getFile(keytabFile);
        if (!userKeytabFile.exists()) {
            LOG.error("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") does not exsit.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") does not exsit.");
        }

        javax.security.auth.login.Configuration.setConfiguration(
            new JaasConfiguration(loginContextName, principal, userKeytabFile.getCanonicalPath()));

        javax.security.auth.login.Configuration conf = javax.security.auth.login.Configuration.getConfiguration();
        if (!(conf instanceof JaasConfiguration)) {
            LOG.error("javax.security.auth.login.Configuration is not JaasConfiguration.");
            throw new IOException("javax.security.auth.login.Configuration is not JaasConfiguration.");
        }

        AppConfigurationEntry[] entrys = conf.getAppConfigurationEntry(loginContextName);
        if (entrys == null) {
            LOG.error(
                "javax.security.auth.login.Configuration has no AppConfigurationEntry named " + loginContextName + ".");
            throw new IOException(
                "javax.security.auth.login.Configuration has no AppConfigurationEntry named " + loginContextName + ".");
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
                if (entrys[i].getOptions().get("keyTab").equals(keytabFile)) {
                    checkKeytab = true;
                }
            }

        }

        if (!checkPrincipal) {
            LOG.error(
                "AppConfigurationEntry named " + loginContextName + " does not have principal value of " + principal
                    + ".");
            throw new IOException(
                "AppConfigurationEntry named " + loginContextName + " does not have principal value of " + principal
                    + ".");
        }

        if (!checkKeytab) {
            LOG.error("AppConfigurationEntry named " + loginContextName + " does not have keyTab value of " + keytabFile
                + ".");
            throw new IOException(
                "AppConfigurationEntry named " + loginContextName + " does not have keyTab value of " + keytabFile
                    + ".");
        }

    }

    /**
     * copy from hbase zkutil 0.94&0.98 A JAAS configuration that defines the login modules that we want to use for
     * login.
     */
    private static class JaasConfiguration extends javax.security.auth.login.Configuration {
        private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<String, String>();

        private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap<String, String>();

        private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN = new AppConfigurationEntry(
            KerberosUtil.getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            KEYTAB_KERBEROS_OPTIONS);

        private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF = new AppConfigurationEntry[] {
            KEYTAB_KERBEROS_LOGIN
        };

        static {
            String jaasEnvVar = System.getenv("HBASE_JAAS_DEBUG");
            if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
                BASIC_JAAS_OPTIONS.put("debug", "true");
            }
        }

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

        private final String loginContextName;

        private final boolean useTicketCache;

        private final String keytabFile;

        private final String principal;

        private javax.security.auth.login.Configuration baseConfig;

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
