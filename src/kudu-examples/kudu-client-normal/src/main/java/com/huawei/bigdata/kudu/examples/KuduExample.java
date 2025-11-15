package com.huawei.bigdata.kudu.examples;

import com.huawei.bigdata.kudu.examples.KuduUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.ListTablesResponse;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;


public class KuduExample {
    public static void main(String[] args) {
        String kuduMaster = System.getProperty("kuduMasters", "ip:7051,ip:7051,ip:7051");
        try {
            KuduClient kuduClient = UserGroupInformation.getLoginUser().doAs(
                    new PrivilegedExceptionAction<KuduClient>() {
                        @Override
                        public KuduClient run() throws Exception {
                            return new KuduClient.KuduClientBuilder(kuduMaster).defaultAdminOperationTimeoutMs(10000).build();
                        }
                    }
            );

            ListTablesResponse tableList = kuduClient.getTablesList();
            tableList.getTableInfosList().forEach(System.out::println);

            String tableName = "test";
            //删除KUDU表
            KuduUtils.dropTable(kuduClient, tableName);
            //创建KUDU表
            KuduUtils.createTable(kuduClient, tableName);
            //列出KUDU表
            KuduUtils.tableList(kuduClient);
            //插入KUDU表
            KuduUtils.insert(kuduClient, tableName, 100);
            KuduUtils.upsert(kuduClient, tableName, 100);
            //删除KUDU数据
            KuduUtils.delete(kuduClient, tableName, 100);
            //扫描KUDU表
            KuduUtils.scanerTable(kuduClient, tableName);

            try {
                kuduClient.close();
            } catch (KuduException e) {
                System.out.printf("KuduClient close fail.", e);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}