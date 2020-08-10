package com.huawei.bigdata.spark.examples

import java.io.File

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

object FemaleInfoCollection {
  // Table structure, used for mapping the text data to df 
  case class FemaleInfo(name: String, gender: String, stayTime: Int)

  def main(args: Array[String]) {

    val hadoopConf: Configuration  = new Configuration()
    if("kerberos".equalsIgnoreCase(hadoopConf.get("hadoop.security.authentication"))) {
      //security mode

      val userPrincipal = "sparkuser"
      val USER_KEYTAB_FILE = "user.keytab"
      val filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator
      val krbFile = filePath + "krb5.conf"
      val userKeyTableFile = filePath + USER_KEYTAB_FILE
      LoginUtil.login(userPrincipal, userKeyTableFile, krbFile, hadoopConf)
    }

    // Configure Spark application name
    val sparkConf = new SparkConf().setAppName("FemaleInfo")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    // Convert RDD to DataFrame through the implicit conversion, then register table.
    sc.textFile(args(0)).map(_.split(","))
      .map(p => FemaleInfo(p(0), p(1), p(2).trim.toInt))
      .toDF.registerTempTable("FemaleInfoTable")

    // Via SQL statements to screen out the time information of female stay on the Internet , and aggregated the same names.
    val femaleTimeInfo = sqlContext.sql("select name,sum(stayTime) as " +
      "stayTime from FemaleInfoTable where gender = 'female' group by name")

    // Filter information about female netizens who spend more than 2 hours online.
    val c = femaleTimeInfo.filter("stayTime >= 120").collect().foreach(println)
    sc.stop()  }
}
