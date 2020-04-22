package com.huawei.bigdata.security;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.log4j.Logger;

public class LoginUtil
{
    
    private static final Logger LOG = Logger.getLogger(LoginUtil.class);
    
    private static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private static final String ZOOKEEPER_SERVER_PRINCIPAL_KEY = "zookeeper.server.principal";

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
    
    public synchronized static void login(String userPrincipal, String userKeytabPath, String krb5ConfPath, Configuration conf)
        throws IOException
    {
        // 1.check input parameters
        if ((userPrincipal == null) || (userPrincipal.length() <= 0))
        {
            LOG.error("input userPrincipal is invalid.");
            throw new IOException("input userPrincipal is invalid.");
        }
        
        if ((userKeytabPath == null) || (userKeytabPath.length() <= 0))
        {
            LOG.error("input userKeytabPath is invalid.");
            throw new IOException("input userKeytabPath is invalid.");
        }
                
        if ((krb5ConfPath == null) || (krb5ConfPath.length() <= 0))
        {
            LOG.error("input krb5ConfPath is invalid.");
            throw new IOException("input krb5ConfPath is invalid.");
        }
        
        if ((conf == null))
        {
            LOG.error("input conf is invalid.");
            throw new IOException("input conf is invalid.");
        }
        
        // 2.check file exsits
        File userKeytabFile = new File(userKeytabPath);
        if (!userKeytabFile.exists())
        {
            LOG.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") does not exsit.");
        }
        if (!userKeytabFile.isFile())
        {
            LOG.error("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");
            throw new IOException("userKeytabFile(" + userKeytabFile.getAbsolutePath() + ") is not a file.");
        }
        
        File krb5ConfFile = new File(krb5ConfPath);
        if (!krb5ConfFile.exists())
        {
            LOG.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exsit.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") does not exsit.");
        }
        if (!krb5ConfFile.isFile())
        {
            LOG.error("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");
            throw new IOException("krb5ConfFile(" + krb5ConfFile.getAbsolutePath() + ") is not a file.");
        }
        
        // 3.set and check krb5config
        setKrb5Config(krb5ConfFile.getAbsolutePath());        
        setConfiguration(conf);
        
        // 4.login and check for hadoop
        loginHadoop(userPrincipal, userKeytabFile.getAbsolutePath());
        
        LOG.info("Login success!!!!!!!!!!!!!!");
    }
    
    private static void setConfiguration(Configuration conf) throws IOException {
	    UserGroupInformation.setConfiguration(conf);
    }
        
    private static boolean checkNeedLogin(String principal)
        throws IOException
    {
        if (!UserGroupInformation.isSecurityEnabled())
        {
            LOG.error("UserGroupInformation is not SecurityEnabled, please check if core-site.xml exists in classpath.");
            throw new IOException(
                "UserGroupInformation is not SecurityEnabled, please check if core-site.xml exists in classpath.");
        }
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if ((currentUser != null) && (currentUser.hasKerberosCredentials()))
        {
            if (checkCurrentUserCorrect(principal))
            {
                LOG.info("current user is " + currentUser + "has logined.");
                if (!currentUser.isFromKeytab())
                {
                    LOG.error("current user is not from keytab.");
                    throw new IOException("current user is not from keytab.");
                }
                return false;
            }
            else
            {
                LOG.error("current user is " + currentUser + "has logined. please check your enviroment , especially when it used IBM JDK or kerberos for OS count login!!");
                throw new IOException("current user is " + currentUser + " has logined. And please check your enviroment!!");
            }
        }
        
        return true;
    }
    
    private static void setKrb5Config(String krb5ConfFile)
        throws IOException
    {
        System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, krb5ConfFile);
        String ret = System.getProperty(JAVA_SECURITY_KRB5_CONF_KEY);
        if (ret == null)
        {
            LOG.error(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is null.");
        }
        if (!ret.equals(krb5ConfFile))
        {
            LOG.error(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
            throw new IOException(JAVA_SECURITY_KRB5_CONF_KEY + " is " + ret + " is not " + krb5ConfFile + ".");
        }
    }
    
    private static void loginHadoop(String principal, String keytabFile)
        throws IOException
    {
        try
        {
            UserGroupInformation.loginUserFromKeytab(principal, keytabFile);
        }
        catch (IOException e)
        {
            LOG.error("login failed with " + principal + " and " + keytabFile + ".");
            LOG.error("perhaps cause 1 is " + LOGIN_FAILED_CAUSE_PASSWORD_WRONG + ".");
            LOG.error("perhaps cause 2 is " + LOGIN_FAILED_CAUSE_TIME_WRONG + ".");
            LOG.error("perhaps cause 3 is " + LOGIN_FAILED_CAUSE_AES256_WRONG + ".");
            LOG.error("perhaps cause 4 is " + LOGIN_FAILED_CAUSE_PRINCIPAL_WRONG + ".");
            LOG.error("perhaps cause 5 is " + LOGIN_FAILED_CAUSE_TIME_OUT + ".");
            
            throw e;
        }
    }
    
    private static void checkAuthenticateOverKrb()
        throws IOException
    {
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        if (loginUser == null)
        {
            LOG.error("current user is " + currentUser + ", but loginUser is null.");
            throw new IOException("current user is " + currentUser + ", but loginUser is null.");
        }
        if (!loginUser.equals(currentUser))
        {
            LOG.error("current user is " + currentUser + ", but loginUser is " + loginUser + ".");
            throw new IOException("current user is " + currentUser + ", but loginUser is " + loginUser + ".");
        }
        if (!loginUser.hasKerberosCredentials())
        {
            LOG.error("current user is " + currentUser + " has no Kerberos Credentials.");
            throw new IOException("current user is " + currentUser + " has no Kerberos Credentials.");
        }
        if (!UserGroupInformation.isLoginKeytabBased())
        {
            LOG.error("current user is " + currentUser + " is not Login Keytab Based.");
            throw new IOException("current user is " + currentUser + " is not Login Keytab Based.");
        }
    }
    
    private static boolean checkCurrentUserCorrect(String principal)
        throws IOException
    {
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        if (ugi == null)
        {
            LOG.error("current user still null.");
            throw new IOException("current user still null.");
        }
        
        String defaultRealm = null;
		try {
			defaultRealm = KerberosUtil.getDefaultRealm();
		} catch (Exception e) {
			LOG.warn("getDefaultRealm failed.");
			throw new IOException(e);
		}

        if ((defaultRealm != null) && (defaultRealm.length() > 0))
        {
            StringBuilder realm = new StringBuilder();
            StringBuilder principalWithRealm = new StringBuilder();
            realm.append("@").append(defaultRealm);
            if (!principal.endsWith(realm.toString()))
            {
                principalWithRealm.append(principal).append(realm);
                principal = principalWithRealm.toString();
            }
        }
        
        return principal.equals(ugi.getUserName());
    }
}
