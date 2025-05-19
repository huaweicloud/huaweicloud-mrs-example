/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2012-2021. All rights reserved.
 */

package com.huawei.bigdata.spark

import com.huawei.bigdata.hadoop.security.{KerberosUtil, LoginUtil}
import com.huawei.bigdata.utils.PropertiesCache
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic.{HADOOP_SECURITY_AUTHENTICATION, HADOOP_SECURITY_AUTHORIZATION}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


/**
 * Utils for spark
 *
 * @since 2021-01-25
 */
object SparkUtil {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def getSparkSession(): SparkSession = {
    val session = SparkSession
      .builder()
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()
    session
  }

  def login():Unit = {
    val path = System.getProperty("user.dir")
    val userKeytabFile = path + "/developuser.keytab"
    val krb5File = path+ "/krb5.conf"

    val user = PropertiesCache.getInstance().getProperty("submit_user")

    val conf = new Configuration
    conf.set("username.client.kerberos.principal", user)
    conf.set("username.client.keytab.file", userKeytabFile)
    conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos")
    conf.set(HADOOP_SECURITY_AUTHORIZATION, "true")

    val ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL = "zookeeper" + "/hadoop." + KerberosUtil.getKrb5DomainRealm

    /*
     * if need to connect zk, please provide jaas info about zk. of course,
     * you can do it as below:
     * System.setProperty("java.security.auth.login.config", confDirPath +
     * "jaas.conf"); but the demo can help you more : Note: if this process
     * will connect more than one zk cluster, the demo may be not proper. you
     * can contact us for more help
     */
    LoginUtil.setJaasConf("Client", user, userKeytabFile)
    LoginUtil.setZookeeperServerPrincipal("zookeeper.server.principal", ZOOKEEPER_DEFAULT_SERVER_PRINCIPAL)
    LoginUtil.login(user, userKeytabFile, krb5File, conf)
    logger.info("login user: {}" , UserGroupInformation.getLoginUser)
  }

}
