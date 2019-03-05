package com.huawei.storm.example.common;

/**
 * 安全模式下的kerberos认证方式
 * 
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
