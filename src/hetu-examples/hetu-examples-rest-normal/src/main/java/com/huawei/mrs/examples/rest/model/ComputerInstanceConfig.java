/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2020-2020. All rights reserved.
 */

package com.huawei.mrs.examples.rest.model;

public class ComputerInstanceConfig {

    /**
     * this is a json example for creating a computer instance api.
     */
    public static String getCreateComputerInstanceConfigExample() {
        String config = "{\n" +
                "  \"flexType\": \"MANUAL\",\n" +
                "  \"tenant\": \"default\",\n" +
                "  \"deployTimeoutSec\": 300,\n" +
                "  \"cnMemory\": 5120,\n" +
                "  \"cn\": 2,\n" +
                "  \"cnVcores\": 1,\n" +
                "  \"workerMemory\": 10240,\n" +
                "  \"initWorker\": 2,\n" +
                "  \"workerVcores\": 1,\n" +
                "  \"openDynamic\": false,\n" +
                "  \"maintenanceInstance\": false,\n" +
                "  \"start\": true,\n" +
                "  \"queryRatio\": 0.7,\n" +
                "  \"dynamicConfig\": {},\n" +
                "  \"seniorConfig\": {\n" +
                "    \"customizeConfig\": {\n" +
                "      \"coordinator.jvm.config\": {\n" +
                "        \"extraJavaOptions\": \"-server\\n-Xmx4G\\n-XX:+UseG1GC\\n-XX:G1HeapRegionSize=32M\\n-XX:+UseGCOverheadLimit\\n-XX:+ExplicitGCInvokesConcurrent\\n-XX:+ExitOnOutOfMemoryError\\n-Dlog.enable-console=false\\n-Djava.security.krb5.conf=CONTAINER_ROOT_PATH/etc/krb5.conf\\n-Djava.security.auth.login.config=CONTAINER_ROOT_PATH/etc/jaas-zk.conf\\n-Dzookeeper.server.principal=zookeeper/hadoop.hadoop.com@HADOOP.COM\\n-Dzookeeper.server.realm=HADOOP.COM\\n-Dzookeeper.sasl.clientconfig=Client\\n-Dzookeeper.auth.type=kerberos\\n-Dscc.configuration.path=/opt/huawei/Bigdata/Bigdata/common/runtime0/securityforscc/config\\n-Dbeetle.application.home.path=/opt/huawei/Bigdata/Bigdata/common/runtime/security/config\\n-Dhadoop.config-dir=CONTAINER_ROOT_PATH/etc\\n-Djdk.tls.ephemeralDHKeySize=4096\\n-Dsun.security.krb5.rcache=none\\n-Dhetu.metastore.config-file=CONTAINER_ROOT_PATH/etc/hetumetastore.properties\\n-Djava.io.tmpdir=CONTAINER_ROOT_PATH/tmp/hetuserver\"\n" +
                "      },\n" +
                "      \"worker.jvm.config\": {\n" +
                "        \"extraJavaOptions\": \"-server\\n-Xmx8G\\n-XX:+UseG1GC\\n-XX:G1HeapRegionSize=32M\\n-XX:+UseGCOverheadLimit\\n-XX:+ExplicitGCInvokesConcurrent\\n-XX:+ExitOnOutOfMemoryError\\n-Dlog.enable-console=false\\n-Djava.security.krb5.conf=CONTAINER_ROOT_PATH/etc/krb5.conf\\n-Djava.security.auth.login.config=CONTAINER_ROOT_PATH/etc/jaas-zk.conf\\n-Dzookeeper.server.principal=zookeeper/hadoop.hadoop.com@HADOOP.COM\\n-Dzookeeper.server.realm=HADOOP.COM\\n-Dzookeeper.sasl.clientconfig=Client\\n-Dzookeeper.auth.type=kerberos\\n-Dscc.configuration.path=/opt/huawei/Bigdata/Bigdata/common/runtime0/securityforscc/config\\n-Dbeetle.application.home.path=/opt/huawei/Bigdata/Bigdata/common/runtime/security/config\\n-Dhadoop.config-dir=CONTAINER_ROOT_PATH/etc\\n-Djdk.tls.ephemeralDHKeySize=4096\\n-Dsun.security.krb5.rcache=none\\n-Dhetu.metastore.config-file=CONTAINER_ROOT_PATH/etc/hetumetastore.properties\\n-Djava.io.tmpdir=CONTAINER_ROOT_PATH/tmp/hetuserver\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"curWorker\": 2,\n" +
                "  \"targetWorker\": 2\n" +
                "}";

        return config;
    }

    /**
     * this is a json example for flex up or flex down the computer instance worker number api.
     */
    public static String getFlexComputerInstanceConfigExample() {
        String config = "{\n" +
                "  \"flexType\": \"MANUAL\",\n" +
                "  \"tenant\": \"default\",\n" +
                "  \"deployTimeoutSec\": 300,\n" +
                "  \"cnMemory\": 5120,\n" +
                "  \"cn\": 2,\n" +
                "  \"cnVcores\": 1,\n" +
                "  \"workerMemory\": 10240,\n" +
                "  \"initWorker\": 2,\n" +
                "  \"workerVcores\": 1,\n" +
                "  \"openDynamic\": false,\n" +
                "  \"maintenanceInstance\": false,\n" +
                "  \"start\": false,\n" +
                "  \"queryRatio\": 0.7,\n" +
                "  \"dynamicConfig\": {},\n" +
                "  \"seniorConfig\": {\n" +
                "    \"customizeConfig\": {\n" +
                "      \"coordinator.jvm.config\": {\n" +
                "        \"extraJavaOptions\": \"-server\\n-Xmx4G\\n-XX:+UseG1GC\\n-XX:G1HeapRegionSize=32M\\n-XX:+UseGCOverheadLimit\\n-XX:+ExplicitGCInvokesConcurrent\\n-XX:+ExitOnOutOfMemoryError\\n-Dlog.enable-console=false\\n-Djava.security.krb5.conf=CONTAINER_ROOT_PATH/etc/krb5.conf\\n-Djava.security.auth.login.config=CONTAINER_ROOT_PATH/etc/jaas-zk.conf\\n-Dzookeeper.server.principal=zookeeper/hadoop.hadoop.com@HADOOP.COM\\n-Dzookeeper.server.realm=HADOOP.COM\\n-Dzookeeper.sasl.clientconfig=Client\\n-Dzookeeper.auth.type=kerberos\\n-Dscc.configuration.path=/opt/huawei/Bigdata/Bigdata/common/runtime0/securityforscc/config\\n-Dbeetle.application.home.path=/opt/huawei/Bigdata/Bigdata/common/runtime/security/config\\n-Dhadoop.config-dir=CONTAINER_ROOT_PATH/etc\\n-Djdk.tls.ephemeralDHKeySize=4096\\n-Dsun.security.krb5.rcache=none\\n-Dhetu.metastore.config-file=CONTAINER_ROOT_PATH/etc/hetumetastore.properties\\n-Djava.io.tmpdir=CONTAINER_ROOT_PATH/tmp/hetuserver\"\n" +
                "      },\n" +
                "      \"worker.jvm.config\": {\n" +
                "        \"extraJavaOptions\": \"-server\\n-Xmx8G\\n-XX:+UseG1GC\\n-XX:G1HeapRegionSize=32M\\n-XX:+UseGCOverheadLimit\\n-XX:+ExplicitGCInvokesConcurrent\\n-XX:+ExitOnOutOfMemoryError\\n-Dlog.enable-console=false\\n-Djava.security.krb5.conf=CONTAINER_ROOT_PATH/etc/krb5.conf\\n-Djava.security.auth.login.config=CONTAINER_ROOT_PATH/etc/jaas-zk.conf\\n-Dzookeeper.server.principal=zookeeper/hadoop.hadoop.com@HADOOP.COM\\n-Dzookeeper.server.realm=HADOOP.COM\\n-Dzookeeper.sasl.clientconfig=Client\\n-Dzookeeper.auth.type=kerberos\\n-Dscc.configuration.path=/opt/huawei/Bigdata/Bigdata/common/runtime0/securityforscc/config\\n-Dbeetle.application.home.path=/opt/huawei/Bigdata/Bigdata/common/runtime/security/config\\n-Dhadoop.config-dir=CONTAINER_ROOT_PATH/etc\\n-Djdk.tls.ephemeralDHKeySize=4096\\n-Dsun.security.krb5.rcache=none\\n-Dhetu.metastore.config-file=CONTAINER_ROOT_PATH/etc/hetumetastore.properties\\n-Djava.io.tmpdir=CONTAINER_ROOT_PATH/tmp/hetuserver\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"curWorker\": 2,\n" +
                "  \"targetWorker\": 3\n" +
                "}";

        return config;
    }
}
