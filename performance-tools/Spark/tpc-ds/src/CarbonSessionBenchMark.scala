//package org.apache.carbondata.examples

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.CarbonSession._

object CarbonSessionBenchMark {

    def main(args: Array[String]) {
        //System.setProperty("carbon.properties.filepath", "/opt/huawei/Bigdata/FusionInsight_SparkV2_V100R002C70/install/spark/cfgV2/carbon.properties")
        
        val spark2 = SparkSession.builder().enableHiveSupport().getOrCreateCarbonSession("hdfs://hacluster/user/hive/warehouse/carbon.store")
        spark2.sparkContext.setLogLevel("INFO")
        
        
        val create_db_sql = "create database if not exists " +  args(0)
        val use_db_sql = "use " +  args(0)
        val exe_sql =  Source.fromFile(args(1)).mkString

        println("create_db_sql :" + create_db_sql)
        spark2.sql(create_db_sql).show
        
        println("use_db_sql :" + use_db_sql)
        spark2.sql(use_db_sql).show
        
        println("exe_sql :" + exe_sql)
        spark2.sql(exe_sql).show
        
        spark2.stop()
    }
}

