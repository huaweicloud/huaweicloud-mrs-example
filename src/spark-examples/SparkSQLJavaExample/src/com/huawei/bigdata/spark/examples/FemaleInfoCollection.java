package com.huawei.bigdata.spark.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.huawei.hadoop.security.LoginUtil;

import java.io.File;
import java.io.IOException;
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
        Configuration hadoopConf = new Configuration();
        if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))){
            //security mode

            final String userPrincipal = "sparkuser";
            final String USER_KEYTAB_FILE = "user.keytab";
            String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
            String krbFile = filePath + "krb5.conf";
            String userKeyTableFile = filePath + USER_KEYTAB_FILE;

            LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf);
        }

        SparkConf conf = new SparkConf().setAppName("CollectFemaleInfo");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);

        // Convert RDD to DataFrame through the implicit conversion.
        JavaRDD<FemaleInfo> femaleInfoJavaRDD = jsc.textFile(args[0]).map(
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
    Dataset<Row> schemaFemaleInfo = sqlContext.createDataFrame(femaleInfoJavaRDD, FemaleInfo.class);
    schemaFemaleInfo.registerTempTable("FemaleInfoTable");

    // Run SQL query
    Dataset<Row> femaleTimeInfo = sqlContext.sql("select * from " +
        "(select name,sum(stayTime) as totalStayTime from FemaleInfoTable " +
        "where gender = 'female' group by name )" +
        " tmp where totalStayTime >120");

       // Collect the columns of a row in the result.
        List<String> result = femaleTimeInfo.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return  row.getString(0) + "," + row.getLong(1);
            }
        }).collect();
        System.out.println(result);
        jsc.stop();
    }
}
