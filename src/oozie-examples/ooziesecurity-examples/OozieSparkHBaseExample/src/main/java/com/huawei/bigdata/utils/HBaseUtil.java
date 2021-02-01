/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.utils;

import com.huawei.bigdata.spark.SparkUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * HBase login user util
 *
 * @since 2021-01-25
 */
public class HBaseUtil {
    /**
     * create login user for HBase connection
     *
     * @return login user
     */
    public static User getAuthenticatedUser() {
        User loginUser = null;
        SparkUtil.login();

        try {
            loginUser =  User.create(UserGroupInformation.getLoginUser());
        } catch (IOException e) {
            e.printStackTrace();
        }

        return loginUser;
    }
}
