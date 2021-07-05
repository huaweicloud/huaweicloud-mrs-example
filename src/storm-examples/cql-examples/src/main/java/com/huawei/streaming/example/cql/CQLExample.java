/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2020. All rights reserved.
 */

package com.huawei.streaming.example.cql;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.Driver;
import com.huawei.streaming.cql.exception.CQLException;

import java.io.File;
import java.util.List;

/**
 * CQL客户端示例
 *
 * @since 2020-08-22
 */
public class CQLExample {
    public static void main(String[] args) throws CQLException {
        new CQLExample().execute();
    }

    /**
     * 读取并执行示例CQL文件
     *
     * @throws CQLException
     */
    public void execute() throws CQLException {
        String cqlFile =
                System.getProperty("user.dir")
                        + File.separator
                        + "src"
                        + File.separator
                        + "main"
                        + File.separator
                        + "resources"
                        + File.separator
                        + "example.cql";
        List<String> cqls = readCQLS(cqlFile);
        executeCQL(cqls);
    }

    /**
     * 执行CQL语句
     * CQL引擎只支持单条CQL语句的提交
     */
    private void executeCQL(List<String> cqls) throws CQLException {
        Driver driver = new Driver();

        setSecurityConfs(driver);

        try {
            for (String cql : cqls) {
                driver.run(cql);
            }
        } finally {
            driver.clean();
        }
    }

    private void setSecurityConfs(Driver driver) throws CQLException {
        StreamingConfig config = new StreamingConfig();
        Object authentication = config.get(StreamingConfig.STREAMING_SECURITY_AUTHENTICATION);

        if (authentication != null && authentication.toString().equalsIgnoreCase("KERBEROS")) {
            /*
             * CQL安全相关的配置参数，其他都已经在stremaing-site.xml中包含
             * 这里就需要配置krb5文件地址和用户的pricipal和keytab文件地址。
             *
             * 用户的pricipal和keytab文件地址需要用户自己手工修改内容
             */
            String confDir =
                    System.getProperty("user.dir")
                            + File.separator
                            + "src"
                            + File.separator
                            + "main"
                            + File.separator
                            + "resources"
                            + File.separator;
            String userPrincipal = "example/hadoop@HADOOP.COM";
            String keytabPath = confDir + "example.keytab";
            String krbfilepath = confDir + "krb5.conf";

            driver.run("set 'streaming.security.user.principal' = '" + userPrincipal + "'");
            driver.run("set 'streaming.security.keytab.path' = '" + keytabPath + "'");
            driver.run("set 'streaming.security.krbconf.path' = '" + krbfilepath + "'");
        }
    }

    /**
     * 从文件中读取CQL语句
     */
    private List<String> readCQLS(String cqlFile) throws CQLException {
        CQLFileReader reader = new CQLFileReader();
        reader.readCQLs(cqlFile);
        return reader.getResult();
    }
}
