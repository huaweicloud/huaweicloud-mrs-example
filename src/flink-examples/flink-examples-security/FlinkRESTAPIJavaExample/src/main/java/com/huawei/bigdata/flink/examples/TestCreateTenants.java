package com.huawei.bigdata.flink.examples;


import com.huawei.bigdata.flink.util.HttpClientUtil;
import com.huawei.bigdata.flink.util.LoginClient;

public class TestCreateTenants {
    public static void main(String[] args) {
        System.setProperty("java.security.krb5.conf", "C:/krb5.conf");
        String principal = "username";
        String keytab = "C:/user.keytab";
        String url = "https://xx.xx.xx.xx:28943/flink/v1/tenants";
        String jsonstr = "{" +
                "\n\t \"tenantId\":\"1\"," +
                "\n\t \"tenantName\":\"test1\"," +
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
