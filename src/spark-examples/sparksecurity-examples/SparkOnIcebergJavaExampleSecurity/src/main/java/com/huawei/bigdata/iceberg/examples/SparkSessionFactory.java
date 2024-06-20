/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package com.huawei.bigdata.iceberg.examples;
import org.apache.spark.sql.SparkSession;

public class SparkSessionFactory
{

    public static SparkSession buildSparkSession(String catalogName) {
        return SparkSession.builder()
                 // if just for test on windows, you should set master to local, like: .master("local)
                .appName("spark on iceberg example")
                .config(
                        "spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog." + catalogName, "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + catalogName + ".type", "hadoop") // 指定catalog 类型, 本地测试建议配hadoop，集群上建议配hive
                .config("spark.sql.catalog." + catalogName + ".warehouse", "/temp/iceberg_warehouse")
                .config("spark.network.timout", 500000)
                .config("spark.sql.storeAssignmentPolicy", "ANSI")
                .getOrCreate();
    }
}
