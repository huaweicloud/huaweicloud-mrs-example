package com.huawei.bigdata.spark.examples

import com.huawei.hadoop.security.LoginUtil
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession

object FemaleInfoCollection {

  // Table structure, used for mapping the text data to df
  case class FemaleInfo(name: String, gender: String, stayTime: Int)

  def main(args: Array[String]) {

    val userPrincipal = "sparkuser"
    val userKeytabPath = "/opt/FIclient/user.keytab"
    val krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf"
    val hadoopConf: Configuration = new Configuration()
    LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

    // Configure Spark application name
    val spark = SparkSession
      .builder()
      .appName("FemaleInfo")
      .getOrCreate()
    import spark.implicits._
    // Convert RDD to DataFrame through the implicit conversion, then register table.
    spark.sparkContext.textFile(args(0)).map(_.split(","))
      .map(p => FemaleInfo(p(0), p(1), p(2).trim.toInt))
      .toDF.registerTempTable("FemaleInfoTable")

    // Via SQL statements to screen out the time information of female stay on the Internet , and aggregated the same names.
    val femaleTimeInfo = spark.sql("select name,sum(stayTime) as " +
      "stayTime from FemaleInfoTable where gender = 'female' group by name")

    // Filter information about female netizens who spend more than 2 hours online.
    val c = femaleTimeInfo.filter("stayTime >= 120").collect().foreach(println)
    spark.stop()
  }
}
