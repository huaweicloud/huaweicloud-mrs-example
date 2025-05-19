package com.huawei.spring.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class LoginUtil {

    protected static final Logger LOG = LoggerFactory.getLogger(LoginUtil.class.getName());


    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    public synchronized static void login(String userPrincipal, String userKeytabPath, String krb5ConfPath,
        Configuration conf) throws IOException {
        // 1.check input parameters
        if ((userPrincipal == null) || (userPrincipal.length() <= 0)) {
            LOG.error("input userPrincipal is invalid.");
            throw new IOException("input userPrincipal is invalid.");
        }

        if ((userKeytabPath == null) || (userKeytabPath.length() <= 0)) {
            LOG.error("input userKeytabPath is invalid.");
            throw new IOException("input userKeytabPath is invalid.");
        }

        if ((krb5ConfPath == null) || (krb5ConfPath.length() <= 0)) {
            LOG.error("input krb5ConfPath is invalid.");
            throw new IOException("input krb5ConfPath is invalid.");
        }

        if ((conf == null)) {
            LOG.error("input conf is invalid.");
            throw new IOException("input conf is invalid.");
        }

        // 2.check file exsits
        File userKeytabFile = new File(userKeytabPath);
        if (!userKeytabFile.exists()) {
            LOG.error("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") does not exsit.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") does not exsit.");
        }
        if (!userKeytabFile.isFile()) {
            LOG.error("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") is not a file.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getCanonicalPath() + ") is not a file.");
        }

        File krb5ConfFile = new File(krb5ConfPath);
        if (!krb5ConfFile.exists()) {
            LOG.error("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") does not exsit.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") does not exsit.");
        }
        if (!krb5ConfFile.isFile()) {
            LOG.error("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") is not a file.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getCanonicalPath() + ") is not a file.");
        }

        // 3.set and check krb5config
        setKrb5Config(krb5ConfFile.getCanonicalPath());
        UserGroupInformation.setConfiguration(conf);

        // 4.login and check for hadoop
        UserGroupInformation.loginUserFromKeytab(userPrincipal,  userKeytabFile.getCanonicalPath());
        LOG.info("Login success!!!!!!!!!!!!!!");
    }


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

}
