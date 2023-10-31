/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.redis.security;

import com.huawei.redis.Const;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * 安全模式的JedisCluster使用方式一: auth.conf文件配置方式
 * 安全模式认证过程较慢，建议使用 带超时时间=15000的构造方法
 *
 * 1. 请先登录Manager管理界面创建Redis的角色以及用户，并下载该用户的客户端认证文件。
 * 2. 将下载的krb5.conf、user.keytab文件放到classpath的config路径下。
 * 3. classpath路径下创建config/auth.conf文件，配置文件内容参见本demo：
 *    userName、keyTabFile、krbConfPath根据实际情况修改。其中keyTabFile、krbConfPath是相对classpath的路径。
 * 4. 若Redis服务是全新安装，需要设置SERVER_REALM环境参数，该参数值为KrbServer服务配置项default_realm的值（该配置可到管理页面查询）。
 *    若Redis服务是由低版本升级而来，则不能设置该参数。
 *
 * @since 2020-09-30
 */
public class SecureJedisClusterDemo {
    /**
     * 入口方法
     *
     * @param args args
     */
    public static void main(String[] args) {
        Set<HostAndPort> hosts = new HashSet<HostAndPort>();
        hosts.add(new HostAndPort(Const.IP_1, Const.PORT_1));
        hosts.add(new HostAndPort(Const.IP_2, Const.PORT_2));

        // add host...
        // System.setProperty("SERVER_REALM","HADOOP.COM");

        JedisCluster client = new JedisCluster(hosts, 15000);
        System.out.println(client.set("SecureJedisClusterDemo", "value"));
        System.out.println(client.get("SecureJedisClusterDemo"));
        client.del("SecureJedisClusterDemo");
        client.close();
    }
}
