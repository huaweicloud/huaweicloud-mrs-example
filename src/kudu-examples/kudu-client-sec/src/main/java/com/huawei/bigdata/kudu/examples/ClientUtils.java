package com.huawei.bigdata.kudu.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class ClientUtils {
    public static void initKerberos(boolean debug) {
        try {
            String krb5ConfPath = "";
            String user = "";
            String keytab = "";

            System.setProperty("java.security.krb5.conf", krb5ConfPath);
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            if (debug) System.setProperty("sun.security.krb5.debug", "true");

            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(configuration);

            UserGroupInformation.loginUserFromKeytab(user, keytab);
            System.out.printf("end UserGroupInformation.isInitialized() : {} , login name : {}",
                    UserGroupInformation.isInitialized(),
                    UserGroupInformation.getLoginUser().getUserName());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("Kerberos login fail.", e);
        }
    }
}
