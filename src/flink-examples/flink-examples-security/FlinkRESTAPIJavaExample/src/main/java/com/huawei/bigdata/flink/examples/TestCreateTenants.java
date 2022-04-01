package com.huawei.bigdata.flink.examples;

import com.huawei.bigdata.flink.util.HttpClientUtil;
import com.huawei.bigdata.flink.util.LoginClient;

import org.apache.flink.api.java.utils.ParameterTool;

public class TestCreateTenants {
    public static void main(String[] args) {
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        final String hostName = paraTool.get("hostName");    // 修改hosts文件，使用主机名
        final String keytab = paraTool.get("keytab");        // user.keytab路径
        final String krb5 = paraTool.get("krb5");            // krb5.conf路径
        final String principal = paraTool.get("principal");  // 认证用户

        System.setProperty("java.security.krb5.conf", krb5);
        String url = "https://"+hostName+":28943/flink/v1/tenants";
        String jsonstr = "{" +
                "\n\t \"tenantId\":\"92\"," +
                "\n\t \"tenantName\":\"test92\"," +
                "\n\t \"remark\":\"test tenant remark1\"," +
                "\n\t \"updateUser\":\"test_updateUser1\"," +
                "\n\t \"createUser\":\"test_createUser1\"" +
                "\n}";

        try {
            LoginClient.getInstance().setConfigure(url, principal, keytab, "");
            LoginClient.getInstance().login();
            System.out.println(HttpClientUtil.doPost(url, jsonstr, "utf-8", true));
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
