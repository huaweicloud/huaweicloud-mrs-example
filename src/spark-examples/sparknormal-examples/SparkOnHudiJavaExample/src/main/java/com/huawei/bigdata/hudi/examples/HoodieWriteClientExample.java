/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata.hudi.examples;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 功能描述:使用hudi执行写入、更新、删除、压缩操作
 *
 * @since 2021-03-17
 */
public class HoodieWriteClientExample {
  private static final Logger LOG = LogManager.getLogger(HoodieWriteClientExample.class);

  private static final String tableType = HoodieTableType.COPY_ON_WRITE.name();

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: HoodieWriteClientExample <tablePath> <tableName>");
      System.exit(1);
    }
    String tablePath = args[0];
    String tableName = args[1];
    SparkConf sparkConf = HoodieExampleSparkUtils.defaultSparkConf("hoodie-client-example");
    SparkRDDWriteClient<HoodieAvroPayload> client = null;
    FileSystem fs = null;
    try (JavaSparkContext jsc = new JavaSparkContext(sparkConf)) {

      // Generator of some records to be loaded in.
      HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

      // initialize the table, if not done already
      Path path = new Path(tablePath);
      fs = FSUtils.getFs(tablePath, jsc.hadoopConfiguration());
      if (!fs.exists(path)) {
        HoodieTableMetaClient.withPropertyBuilder()
            .setTableType(tableType)
            .setTableName(tableName)
            .setPayloadClass(HoodieAvroPayload.class)
            .initTable(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()), tablePath);
      }

      // Create the write client to write some records in
      HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
          .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
          .withDeleteParallelism(2).forTable(tableName)
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
          .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
      client = new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), cfg);

      // inserts
      String newCommitTime = client.startCommit();
      LOG.info("Starting commit " + newCommitTime);

      List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 10);
      List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
      JavaRDD<HoodieRecord<HoodieAvroPayload>> writeRecords = jsc.parallelize(records, 1);
      client.upsert(writeRecords, newCommitTime);

      // updates
      newCommitTime = client.startCommit();
      LOG.info("Starting commit " + newCommitTime);
      List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
      records.addAll(toBeUpdated);
      recordsSoFar.addAll(toBeUpdated);
      writeRecords = jsc.parallelize(records, 1);
      client.upsert(writeRecords, newCommitTime);

      // Delete
      newCommitTime = client.startCommit();
      LOG.info("Starting commit " + newCommitTime);
      // just delete half of the records
      int numToDelete = recordsSoFar.size() / 2;
      List<HoodieKey> toBeDeleted = recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
      JavaRDD<HoodieKey> deleteRecords = jsc.parallelize(toBeDeleted, 1);
      client.delete(deleteRecords, newCommitTime);

      // compaction
      if (HoodieTableType.valueOf(tableType) == HoodieTableType.MERGE_ON_READ) {
        Option<String> instant = client.scheduleCompaction(Option.empty());
        HoodieWriteMetadata<JavaRDD<WriteStatus>> writeStatues = client.compact(instant.get());
        client.commitCompaction(instant.get(), writeStatues.getCommitMetadata().get(), Option.empty());
      }
    } finally {
      if (client != null) {
        client.close();
      }
      if (fs != null) {
        fs.close();
      }
    }
  }

}
