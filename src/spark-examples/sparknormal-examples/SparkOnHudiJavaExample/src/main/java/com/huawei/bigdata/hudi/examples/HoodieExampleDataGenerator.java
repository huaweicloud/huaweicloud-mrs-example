/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata.hudi.examples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * 功能描述
 * 生成模拟数据
 *
 * @since 2021-03-17
 */

public class HoodieExampleDataGenerator<T extends HoodieRecordPayload<T>> {
    private static final String DEFAULT_FIRST_PARTITION_PATH = "2020/01/01";

    private static final String DEFAULT_SECOND_PARTITION_PATH = "2020/01/02";

    private static final String DEFAULT_THIRD_PARTITION_PATH = "2020/01/03";

    private static final String[] DEFAULT_PARTITION_PATHS =
        {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH, DEFAULT_THIRD_PARTITION_PATH};

    /**
     * 生成数据的schema
     */
    public static final String TRIP_EXAMPLE_SCHEMA =
        "{\"type\": \"record\",\"name\": \"triprec\",\"fields\": [ {\"name\": \"ts\",\"type\":"
            + " \"long\"},{\"name\": \"uuid\", \"type\": \"string\"},{\"name\": \"rider\", \"type\":"
            + " \"string\"},{\"name\": \"driver\", \"type\": \"string\"},{\"name\": \"begin_lat\","
            + " \"type\": \"double\"},{\"name\": \"begin_lon\", \"type\": \"double\"},{\"name\":"
            + " \"end_lat\", \"type\": \"double\"},{\"name\": \"end_lon\", \"type\":"
            + " \"double\"},{\"name\":\"fare\",\"type\": \"double\"}]}";

    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);

    private static Random rand = new Random(46474747);

    private final Map<Integer, KeyPartition> existingKeys;

    private final String[] partitionPaths;

    private int numExistingKeys;

    public HoodieExampleDataGenerator(String[] partitionPaths) {
        this(partitionPaths, new HashMap<>());
    }

    public HoodieExampleDataGenerator() {
        this(DEFAULT_PARTITION_PATHS);
    }

    public HoodieExampleDataGenerator(String[] partitionPaths, Map<Integer, KeyPartition> keyPartitionMap) {
        this.partitionPaths = Arrays.copyOf(partitionPaths, partitionPaths.length);
        this.existingKeys = keyPartitionMap;
    }

    /**
     * Generates a new avro record of the above schema format, retaining the key if optionally provided.
     *
     * @param HoodieKey key
     * @param String commitTime
     * @return T
     */
    @SuppressWarnings("unchecked")
    public T generateRandomValue(HoodieKey key, String commitTime) {
        GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + commitTime, "driver-" + commitTime, 0);
        return (T) new HoodieAvroPayload(Option.of(rec));
    }

    private GenericRecord generateGenericRecord(String rowKey, String riderName, String driverName, long timestamp) {
        GenericRecord rec = new GenericData.Record(AVRO_SCHEMA);
        rec.put("uuid", rowKey);
        rec.put("ts", timestamp);
        rec.put("rider", riderName);
        rec.put("driver", driverName);
        rec.put("begin_lat", rand.nextDouble());
        rec.put("begin_lon", rand.nextDouble());
        rec.put("end_lat", rand.nextDouble());
        rec.put("end_lon", rand.nextDouble());
        rec.put("fare", rand.nextDouble() * 100);
        return rec;
    }

    /**
     * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
     *
     * @param String commitTime
     * @param Integer n
     * @return List<HoodieRecord < T>>
     */
    public List<HoodieRecord<T>> generateInserts(String commitTime, Integer integer) {
        return generateInsertsStream(commitTime, integer).collect(Collectors.toList());
    }

    /**
     * Generates new inserts, uniformly across the partition paths above. It also updates the list of existing keys.
     *
     * @param String commitTime
     * @param Integer integer
     * @return Stream<HoodieRecord < T>>
     */
    private Stream<HoodieRecord<T>> generateInsertsStream(String commitTime, Integer integer) {
        int currSize = getNumExistingKeys();
        Stream<Integer> boxed = IntStream.range(0, integer).boxed();
        return boxed.map(it -> {
            String partitionPath = partitionPaths[rand.nextInt(partitionPaths.length)];
            HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
            KeyPartition kp = new KeyPartition();
            kp.key = key;
            kp.partitionPath = partitionPath;
            existingKeys.put(currSize + it, kp);
            numExistingKeys++;
            return new HoodieAvroRecord<>(key, generateRandomValue(key, commitTime));
        });
    }

    /**
     * Generates new updates, randomly distributed across the keys above. There can be duplicates within the returned
     * list
     *
     * @param commitTime Commit Timestamp
     * @param n Number of updates (including dups)
     * @return list of hoodie record updates
     */
    public List<HoodieRecord<T>> generateUpdates(String commitTime, Integer n) {
        List<HoodieRecord<T>> updates = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            KeyPartition kp = existingKeys.get(rand.nextInt(numExistingKeys - 1));
            HoodieRecord<T> record = generateUpdateRecord(kp.key, commitTime);
            updates.add(record);
        }
        return updates;
    }

    private HoodieRecord<T> generateUpdateRecord(HoodieKey key, String commitTime) {
        return new HoodieAvroRecord<>(key, generateRandomValue(key, commitTime));
    }

    private Option<String> convertToString(HoodieRecord<T> record) {
        try {
            String str =
                HoodieAvroUtils.bytesToAvro(((HoodieAvroPayload) record.getData()).getRecordBytes(), AVRO_SCHEMA)
                    .toString();
            str = "{" + str.substring(str.indexOf("\"ts\":"));
            return Option.of(str.replaceAll("}", ", \"partitionpath\": \"" + record.getPartitionPath() + "\"}"));
        } catch (IOException e) {
            return Option.empty();
        }
    }

    /**
     * convertToStringList
     *
     * @param List<HoodieRecord<T>> records
     * @return List<String>
     */
    public List<String> convertToStringList(List<HoodieRecord<T>> records) {
        return records.stream()
            .map(this::convertToString)
            .filter(Option::isPresent)
            .map(Option::get)
            .collect(Collectors.toList());
    }

    public int getNumExistingKeys() {
        return numExistingKeys;
    }

    private static class KeyPartition implements Serializable {
        HoodieKey key;

        String partitionPath;
    }

    /**
     * close existingKeys
     */
    public void close() {
        existingKeys.clear();
    }
}
