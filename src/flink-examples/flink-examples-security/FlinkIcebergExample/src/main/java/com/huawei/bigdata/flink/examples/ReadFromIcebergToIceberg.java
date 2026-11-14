/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.source.FlinkSource;

import java.util.HashMap;
import java.util.Map;

public class ReadFromIcebergToIceberg {
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "flink run --class com.huawei.bigdata.flink.examples.ReadFromIcebergToIceberg /opt/FlinkIcebergExample.jar --catalogName  hive_iceberg_catalog"
                        + "  --hive.metastore.uris thrift://xxx.xxx.xxx.xxx:xx,thrift://xxx.xxx.xxx.xxx:xx "
                        + "  --sourceTableName icebergsource  --sinkTableName icebergsink    --hive.metastore.kerberos.principal xxxx  ");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.enableCheckpointing(5000);

        ParameterTool paraTool = ParameterTool.fromArgs(args);
        String catalogName = paraTool.get("catalogName", "hive_iceberg_catalog");
        String hiveMetastoreUri = paraTool.get("hive.metastore.uris"); // 替换为你的 Hive Metastore 地址
        String sourceName = paraTool.get("sourceTableName", "icebergsource");
        String sinkName = paraTool.get("sinkTableName", "icebergsink");
        String hiveMetastorePrincipal = paraTool.get("hive.metastore.kerberos.principal"); // 替换为 Hive Metastore principle
        String warehouse = paraTool.get("warehouse", "hdfs://hacluster/user/hive/warehouse"); // 替换为 Hive Warehouse 路径

        // 2. 配置 Hive Catalog 参数
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("type", "hive");
        catalogProps.put("uri", hiveMetastoreUri);
        catalogProps.put("warehouse", warehouse);
        Configuration conf = new Configuration();
        conf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");
        conf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, hiveMetastorePrincipal);
        CatalogLoader catalogLoader = CatalogLoader.hive(catalogName, conf, catalogProps);
        TableIdentifier table = TableIdentifier.of("default", sourceName);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, table);

        DataStream<RowData> batch = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(false)
                .build();

        DataStream<RowData> readStreamfile = batch.filter(new FilterFunction<RowData>() {
            @Override
            public boolean filter(RowData rowData) throws Exception {
                return rowData.getInt(2) > 100;
            }
        });

        TableIdentifier table2 = TableIdentifier.of("default", sinkName);
        TableLoader tableLoader2 = TableLoader.fromCatalog(catalogLoader, table2);

        FlinkSink.forRowData(readStreamfile)
                .tableLoader(tableLoader2)
                .overwrite(true)
                .append();

        env.execute();
    }
}
