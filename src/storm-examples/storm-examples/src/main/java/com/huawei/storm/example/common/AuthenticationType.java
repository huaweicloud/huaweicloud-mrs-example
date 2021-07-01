/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.storm.example.common;

/**
 * 安全模式下的kerberos认证方式
 *
 * @since 2020-09-30
 */
public enum AuthenticationType {
    /**
     * 登录方式为使用keytab文件登录,推荐使用
     */
    KEYTAB,

    /**
     * 登录方式为使用TicketCache登录,不推荐使用
     */
    TICKET_CACHE;
}
