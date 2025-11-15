package com.huawei.bigdata.kudu.examples;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class KuduUtils {

    /**
     * create操作
     * 传入客户端连接client和表名tableName
     */
    public static void createTable(KuduClient client, String tableName) {
        List<ColumnSchema> columns = new ArrayList<>();
        //添加列并指定每个列的属性
        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).key(false).compressionAlgorithm(ColumnSchema.CompressionAlgorithm.SNAPPY).build());
        Schema schema = new Schema(columns);
        CreateTableOptions createTableOptions = new CreateTableOptions();
        List<String> hashKeys = new ArrayList<>();
        hashKeys.add("id");
        int numBuckets = 8;
        createTableOptions.addHashPartitions (hashKeys, numBuckets);

        try {
            if (!client.tableExists(tableName)) {
                client.createTable(tableName, schema, createTableOptions);
            }
            System.out.println("成功创建Kudu表:" + tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }


    /**
     * upsert操作
     * 传入客户端连接client和表名tableName
     * 预留numRows在多行写入时使用
     */
    public static void upsert(KuduClient client, String tableName, int numRows) {
        try {
            KuduTable kuduTable = client.openTable(tableName);
            KuduSession kuduSession = client.newSession();

            Upsert upsert = kuduTable.newUpsert();
            PartialRow row = upsert.getRow();
            row.addInt("id", 1);
            row.addString("name", "123");
            kuduSession.apply(upsert);
            //手动提交数据
            kuduSession.flush();
            kuduSession.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * insert操作
     * 传入客户端连接client和表名tableName
     * 预留numRows在多行写入时使用
     */
    public static void insert(KuduClient client, String tableName, int numRows) {
        try {
            KuduTable kuduTable = client.openTable(tableName);
            KuduSession kuduSession = client.newSession();
            kuduSession.setMutationBufferSpace(1000);
            //自动提交数据
            kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

            Insert insert = kuduTable.newInsert();
            PartialRow row = insert.getRow();
            row.addInt("id", 1);
            row.addString("name", "123");
            kuduSession.apply(insert);

            kuduSession.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * delete操作
     * 传入客户端连接client和表名tableName
     * 预留numRows在多行写入时使用
     */
    public static void delete(KuduClient client, String tableName, int numRows) {
        try {
            KuduTable kuduTable = client.openTable(tableName);
            KuduSession kuduSession = client.newSession();
            kuduSession.setMutationBufferSpace(1000);

            Delete delete = kuduTable.newDelete();
            PartialRow row = delete.getRow();
            //删除数据需要指定主键
            row.addInt("id", 1);
            kuduSession.apply(delete);

            kuduSession.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * select操作
     * 传入客户端连接client和表名tableName
     */
    public static void scanerTable(KuduClient client, String tableName) {
        try {
            KuduTable kuduTable = client.openTable(tableName);
            KuduScanner kuduScanner = client.newScannerBuilder(kuduTable).setProjectedColumnNames(Arrays.asList("id", "name")).build();
            while(kuduScanner.hasMoreRows()) {
                RowResultIterator rowResultIterator =kuduScanner.nextRows();
                while (rowResultIterator.hasNext()) {
                    RowResult rowResult = rowResultIterator.next();
                    int id = rowResult.getInt("id");
                    String name = rowResult.getString("name");
                    System.out.println("id = " + id + ", name = " + name + ".");
                }
            }
            kuduScanner.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * drop操作
     * 传入客户端连接client和表名tableName
     */
    public static void dropTable(KuduClient client, String tableName) {
        try {
            client.deleteTable(tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

    /**
     * list操作
     * 传入客户端连接client和表名tableName
     */
    public static void tableList(KuduClient client) {
        try {
            ListTablesResponse listTablesResponse = client.getTablesList();
            List<String> tblist = listTablesResponse.getTablesList();
            for(String tableName : tblist) {
                System.out.println(tableName);
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }

}