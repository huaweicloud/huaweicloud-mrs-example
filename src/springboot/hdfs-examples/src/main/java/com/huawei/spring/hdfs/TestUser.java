/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package com.huawei.spring.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

/**
 * @since 2025-01-20
 */
public class TestUser {

    private final static String configDir = "D:\\project\\sample_project\\src\\springboot\\hdfs-examples\\src\\main\\resources";

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        // conf file
        conf.addResource(new Path(configDir + "/hdfs-site.xml"));
        conf.addResource(new Path(configDir + "/core-site.xml"));
        String krb5Conf = configDir + File.separator + "krb5.conf";
        String keytab = configDir + File.separator + "user.keytab";
        UserGroupInformation.setConfiguration(conf);
        System.setProperty("java.security.krb5.conf", krb5Conf);
        UserGroupInformation.loginUserFromKeytab("admintest", keytab);
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        System.out.println(currentUser);
        System.out.println(UserGroupInformation.getLoginUser().getUserName());
        keytab = configDir + File.separator + "test001.keytab";
        UserGroupInformation.loginUserFromKeytab("test001", keytab);
        UserGroupInformation currentUserNew = UserGroupInformation.getCurrentUser();
        System.out.println(currentUserNew.getUserName());
        System.out.println(UserGroupInformation.getLoginUser().getUserName());
        System.out.println("===========");
        // UserGroupInformation.loginUserFromSubject();
        System.out.println(currentUser);
    }

}
