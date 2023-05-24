/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2019. All rights reserved.
 */

package com.huawei.bigdata.hudi.examples;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 功能描述
 * 对获取的数据进行format
 *
 * @since 2021-03-17
 */
public class TransformerExample implements Transformer, Serializable {

    /**
     * format data
     *
     * @param JavaSparkContext jsc
     * @param SparkSession sparkSession
     * @param Dataset<Row> rowDataset
     * @param TypedProperties properties
     * @return Dataset<Row>
     */
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
        TypedProperties properties) {
        JavaRDD<Row> rowJavaRdd = rowDataset.toJavaRDD();
        List<Row> rowList = new ArrayList<>();
        for (Row row : rowJavaRdd.collect()) {
            rowList.add(buildRow(row));
        }
        JavaRDD<Row> stringJavaRdd = jsc.parallelize(rowList);
        List<StructField> fields = new ArrayList<>();
        builFields(fields);
        StructType schema = DataTypes.createStructType(fields);
        Dataset<Row> dataFrame = sparkSession.createDataFrame(stringJavaRdd, schema);
        return dataFrame;
    }

    private void builFields(List<StructField> fields) {
        fields.add(DataTypes.createStructField("age", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("job", DataTypes.StringType, true));
    }

    private Row buildRow(Row row) {
        String age = row.getString(0);
        String id = row.getString(1);
        String job = row.getString(2);
        String name = row.getString(3);
        Row returnRow = RowFactory.create(age, id, job, name);
        return returnRow;
    }
}
