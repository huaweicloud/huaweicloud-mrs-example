package com.huawei.bigdata.spark.examples;

import com.huawei.hadoop.security.LoginUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.List;

public class FemaleInfoCollection {
    public static class FemaleInfo implements Serializable {
        private String name;
        private String gender;
        private Integer stayTime;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public Integer getStayTime() {
            return stayTime;
        }

        public void setStayTime(Integer stayTime) {
            this.stayTime = stayTime;
        }
    }

    public static void main(String[] args) throws Exception {
        String userPrincipal = "sparkuser";
        String userKeytabPath = "/opt/FIclient/user.keytab";
        String krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";
        Configuration hadoopConf = new Configuration();
        LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

        SparkSession spark = SparkSession.builder().appName("CollectFemaleInfo").getOrCreate();

        // Convert RDD to DataFrame through the implicit conversion.
        JavaRDD<FemaleInfo> femaleInfoJavaRDD =
                spark.read()
                        .textFile(args[0])
                        .javaRDD()
                        .map(
                                new Function<String, FemaleInfo>() {
                                    @Override
                                    public FemaleInfo call(String line) throws Exception {
                                        String[] parts = line.split(",");

                                        FemaleInfo femaleInfo = new FemaleInfo();
                                        femaleInfo.setName(parts[0]);
                                        femaleInfo.setGender(parts[1]);
                                        femaleInfo.setStayTime(Integer.parseInt(parts[2].trim()));
                                        return femaleInfo;
                                    }
                                });

        // Register table.
        Dataset<Row> schemaFemaleInfo = spark.createDataFrame(femaleInfoJavaRDD, FemaleInfo.class);
        schemaFemaleInfo.registerTempTable("FemaleInfoTable");

        // Run SQL query
        Dataset<Row> femaleTimeInfo =
                spark.sql(
                        "select * from "
                                + "(select name,sum(stayTime) as totalStayTime from FemaleInfoTable "
                                + "where gender = 'female' group by name )"
                                + " tmp where totalStayTime >120");

        // Collect the columns of a row in the result.
        List<String> result =
                femaleTimeInfo
                        .javaRDD()
                        .map(
                                new Function<Row, String>() {
                                    public String call(Row row) {
                                        return row.getString(0) + "," + row.getLong(1);
                                    }
                                })
                        .collect();
        System.out.println(result);
        spark.stop();
    }
}
