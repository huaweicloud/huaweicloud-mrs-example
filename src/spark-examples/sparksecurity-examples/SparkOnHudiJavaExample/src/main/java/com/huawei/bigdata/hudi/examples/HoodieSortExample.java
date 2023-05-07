/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata.hudi.examples;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

/**
 * 功能描述:使用recordKey按数字排序
 *
 * @since 2021-03-17
 */
public class HoodieSortExample<T extends HoodieRecordPayload>
    implements BulkInsertPartitioner<JavaRDD<HoodieRecord<T>>> {
    @Override
    public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
        JavaPairRDD<String,
            HoodieRecord<T>> stringHoodieRecordJavaPairRDD = records.coalesce(outputSparkPartitions)
                .mapToPair(record -> new Tuple2<>(new StringBuilder().append(record.getPartitionPath())
                    .append("+")
                    .append(record.getRecordKey())
                    .toString(), record));
        JavaRDD<HoodieRecord<T>> hoodieRecordJavaRDD = stringHoodieRecordJavaPairRDD.mapPartitions(partition -> {
            List<Tuple2<String, HoodieRecord<T>>> recordList = new ArrayList<>();
            for (; partition.hasNext();) {
                recordList.add(partition.next());
            }
            Collections.sort(recordList, (o1, o2) -> {
                if (o1._1().split("[+]")[0] == o2._1().split("[+]")[0]) {
                    return Integer.parseInt(o1._1().split("[+]")[1]) - Integer.parseInt(o2._1().split("[+]")[1]);
                } else {
                    return o1._1().split("[+]")[0].compareTo(o2._1().split("[+]")[0]);
                }
            });
            return recordList.stream().map(e -> e._2).iterator();
        });
        return hoodieRecordJavaRDD;
    }

    @Override
    public boolean arePartitionRecordsSorted() {
        return true;
    }
}
