/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata.hudi.examples;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 功能描述:使用hudi执行写入、更新、删除、压缩操作
 *
 * @since 2021-03-17
 */
public class HoodieWriteClientExample {
    private static final Logger LOG = LogManager.getLogger(HoodieWriteClientExample.class);

    private static String tableType = HoodieTableType.COPY_ON_WRITE.name();

    @SuppressWarnings("checkstyle:RegexpMultiline")
    public static void main(String[] args) throws Exception {
        String tablePath = args[args.length - 2];
        String tableName = args[args.length - 1];

        SparkConf sparkConf =
                HoodieExampleSparkUtils.defaultSparkConf("hoodie-client-example");
        SparkRDDWriteClient<HoodieAvroPayload> client = null;
        FileSystem fs = null;
        try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {
            // Generator of some records to be loaded in.
            HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

            // initialize the table, if not done already
            Path path = new Path(tablePath);
            fs = FSUtils.getFs(tablePath, jsc.hadoopConfiguration());
            if (!fs.exists(path)) {
                HoodieTableMetaClient.initTableType(
                        jsc.hadoopConfiguration(),
                        tablePath,
                        HoodieTableType.valueOf(tableType),
                        tableName,
                        HoodieAvroPayload.class.getName());
            }

            // Create the write client to write some records in
            HoodieWriteConfig cfg =
                    HoodieWriteConfig.newBuilder()
                            .withPath(tablePath)
                            .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA)
                            .withParallelism(2, 2)
                            .withDeleteParallelism(2)
                            .forTable(tableName)
                            .withIndexConfig(
                                    HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
                            .withCompactionConfig(
                                    HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build())
                            .build();
            client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), cfg);
            List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>();
            List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>();
            // inserts
            insert(client, dataGen, jsc, records, recordsSoFar);
            // updates
            update(client, dataGen, jsc, records, recordsSoFar);
            // Delete
            delete(client, jsc, recordsSoFar);
            // compaction
            if (HoodieTableType.valueOf(tableType) == HoodieTableType.MERGE_ON_READ) {
                Option<String> instant = client.scheduleCompaction(Option.empty());
                JavaRDD<WriteStatus> writeStatues = client.compact(instant.get());
                client.commitCompaction(instant.get(), writeStatues, Option.empty());
            }
        } finally {
            client.close();
            fs.close();
        }
    }

    private static void delete(
            SparkRDDWriteClient<HoodieAvroPayload> client,
            JavaSparkContext jsc,
            List<HoodieRecord<HoodieAvroPayload>> recordsSoFar) {
        String newCommitTime = client.startCommit();
        // just delete half of the records
        int numToDelete = recordsSoFar.size() / 2;
        List<HoodieKey> toBeDeleted =
                recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
        JavaRDD<HoodieKey> deleteRecords = jsc.parallelize(toBeDeleted, 1);
        client.delete(deleteRecords, newCommitTime);
    }

    private static void update(
            SparkRDDWriteClient<HoodieAvroPayload> client,
            HoodieExampleDataGenerator<HoodieAvroPayload> dataGen,
            JavaSparkContext jsc,
            List<HoodieRecord<HoodieAvroPayload>> records,
            List<HoodieRecord<HoodieAvroPayload>> recordsSoFar) {
        String newCommitTime = client.startCommit();
        List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
        records.addAll(toBeUpdated);
        recordsSoFar.addAll(toBeUpdated);
        JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(records, 1);
        client.upsert(writeRecords, newCommitTime);
    }

    private static void insert(
            SparkRDDWriteClient<HoodieAvroPayload> client,
            HoodieExampleDataGenerator<HoodieAvroPayload> dataGen,
            JavaSparkContext jsc,
            List<HoodieRecord<HoodieAvroPayload>> records,
            List<HoodieRecord<HoodieAvroPayload>> recordsSoFar) {
        String newCommitTime = client.startCommit();
        records.addAll(dataGen.generateInserts(newCommitTime, 10));
        recordsSoFar.addAll(new ArrayList<>(records));
        JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(records, 1);
        client.upsert(writeRecords, newCommitTime);
    }
}
