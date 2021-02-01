/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.hadoop.security;

import org.apache.log4j.Logger;

import java.lang.reflect.Method;

/**
 * Kerberos Utility
 *
 * @since 2020-09-30
 */
public class KerberosUtil {
    private static final Logger logger = Logger.getLogger(KerberosUtil.class);
    /** Java Vender */
    public static final String JAVA_VENDER = "java.vendor";

    /** IBM Flag */
    public static final String IBM_FLAG = "IBM";

    /** Config Class for IBM */
    public static final String CONFIG_CLASS_FOR_IBM = "com.ibm.security.krb5.internal.Config";

    /** Config Class for SUN */
    public static final String CONFIG_CLASS_FOR_SUN = "sun.security.krb5.Config";

    /** GetInstance Method */
    public static final String METHOD_GET_INSTANCE = "getInstance";

    /** GetDefaultRealm Method */
    public static final String METHOD_GET_DEFAULT_REALM = "getDefaultRealm";

    /** Default Realm */
    public static final String DEFAULT_REALM = "HADOOP.COM";

    /**
     * Get Krb5 Domain Realm
     *
     * @return Krb5 Domain Realm
     *
     * @since 2020-09-30
     */
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
            peerRealm = (String) getDefaultRealmMethod.invoke(kerbConf);
            if (logger.isInfoEnabled()) {
                logger.info("Get default realm successfully , the realm is : " + peerRealm);
            }

        } catch (Exception e) {
            peerRealm = DEFAULT_REALM;
            logger.warn("Get default realm failed , use default value : " + DEFAULT_REALM);
        }

        return peerRealm;
    }
}
