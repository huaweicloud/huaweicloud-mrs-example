package com.huawei.bigdata.examples.util;

import org.apache.log4j.Logger;

import java.lang.reflect.Method;

public class KerberosUtil {

    private static Logger logger = Logger.getLogger(KerberosUtil.class);

    public static final String JAVA_VENDER = "java.vendor";
    public static final String IBM_FLAG = "IBM";
    public static final String CONFIG_CLASS_FOR_IBM = "com.ibm.security.krb5.internal.Config";
    public static final String CONFIG_CLASS_FOR_SUN = "sun.security.krb5.Config";
    public static final String METHOD_GET_INSTANCE = "getInstance";
    public static final String METHOD_GET_DEFAULT_REALM = "getDefaultRealm";
    public static final String DEFAULT_REALM = "HADOOP.COM";

    public static String getKrb5DomainRealm() {
        Class<?> krb5ConfClass;
        String peerRealm;
        try {
            if (System.getProperty(JAVA_VENDER).contains(IBM_FLAG)) {
                krb5ConfClass = Class.forName(CONFIG_CLASS_FOR_IBM);
            } else {
                krb5ConfClass = Class.forName(CONFIG_CLASS_FOR_SUN);
            }

            Method getInstanceMethod = krb5ConfClass.getMethod(METHOD_GET_INSTANCE);
            Object kerbConf = getInstanceMethod.invoke(krb5ConfClass);

            Method getDefaultRealmMethod = krb5ConfClass.getDeclaredMethod(METHOD_GET_DEFAULT_REALM);
            peerRealm = (String)getDefaultRealmMethod.invoke(kerbConf);
            logger.info("Get default realm successfully, the realm is : " + peerRealm);

        } catch (Exception e) {
            peerRealm = DEFAULT_REALM;
            logger.warn("Get default realm failed, use default value : " + DEFAULT_REALM);
        }

        return peerRealm;
    }
}
