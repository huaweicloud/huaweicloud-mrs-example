package com.huawei.bigdata.flink.examples;

import com.huawei.bigdata.flink.util.HttpClientUtil;

import org.apache.flink.api.java.utils.ParameterTool;

public class TestCreateTenants {
    public static void main(String[] args) {
        ParameterTool paraTool = ParameterTool.fromArgs(args);
        final String hostName = paraTool.get("hostName");    // 修改hosts文件，使用主机名

        String url = "https://"+hostName+":28943/flink/v1/tenants";
        String jsonstr = "{" +
                "\n\t \"tenantId\":\"101\"," +
                "\n\t \"tenantName\":\"test101\"," +
                "\n\t \"remark\":\"test tenant remark1\"," +
                "\n\t \"updateUser\":\"test_updateUser1\"," +
                "\n\t \"createUser\":\"test_createUser1\"" +
                "\n}";

        try {
            System.out.println(HttpClientUtil.doPost(url, jsonstr, "utf-8", false));
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
