#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved. 版权信息声明
#
# CTBase spark导出样例提交脚本
#
# 支持的参数说明如下:
#   参数1：导出的用户表名
#   参数2：所属聚簇表名
#   参数3：导出使用的索引名
#   参数4：导出路径
#   参数5：导出条件(可不指定，若指定需要给json格式)

#非独立索引表 主索引导出
spark-submit \
--class com.huawei.bigdata.ctbase.examples.CTBaseExportExample \
--master yarn \
--deploy-mode client \
--jars "${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/ctbase-core-0.93.0-h0.cbu.mrs.313.r10.jar,\
"${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/hbase-it-bulk-load-8.1.3-313.r10.jar \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
ctbase-export-examples-0.93.0-h0.cbu.mrs.313.r10.jar \
Consumer_info ClusterTableTest idx_p1 "/tmp/exportByPriIdx"

#非独立索引表 主索引导出+主索引条件
spark-submit \
--class com.huawei.bigdata.ctbase.examples.CTBaseExportExample \
--master yarn \
--deploy-mode client \
--jars "${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/ctbase-core-0.93.0-h0.cbu.mrs.313.r10.jar,\
"${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/hbase-it-bulk-load-8.1.3-313.r10.jar \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
ctbase-export-examples-0.93.0-h0.cbu.mrs.313.r10.jar \
Consumer_info ClusterTableTest idx_p1 "/tmp/exportByPriIdxWithCond" "{'ID':'10002'}"

#非独立索引表 二级索引导出
spark-submit \
--class com.huawei.bigdata.ctbase.examples.CTBaseExportExample \
--master yarn \
--deploy-mode client \
--jars "${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/ctbase-core-0.93.0-h0.cbu.mrs.313.r10.jar,\
"${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/hbase-it-bulk-load-8.1.3-313.r10.jar \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
ctbase-export-examples-0.93.0-h0.cbu.mrs.313.r10.jar \
Consumer_info ClusterTableTest idx_s1 "/tmp/exportBySecIdx"

#非独立索引表 二级索引导出+二级索引条件
spark-submit \
--class com.huawei.bigdata.ctbase.examples.CTBaseExportExample \
--master yarn \
--deploy-mode client \
--jars "${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/ctbase-core-0.93.0-h0.cbu.mrs.313.r10.jar,\
"${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/hbase-it-bulk-load-8.1.3-313.r10.jar \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
ctbase-export-examples-0.93.0-h0.cbu.mrs.313.r10.jar \
Consumer_info ClusterTableTest idx_s1 "/tmp/exportBySecIdxWithCond" "{'NAME':'zhangsan2'}"

#独立索引表 主索引导出
spark-submit \
--class com.huawei.bigdata.ctbase.examples.CTBaseExportExample \
--master yarn \
--deploy-mode client \
--jars "${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/ctbase-core-0.93.0-h0.cbu.mrs.313.r10.jar,\
"${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/hbase-it-bulk-load-8.1.3-313.r10.jar \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
ctbase-export-examples-0.93.0-h0.cbu.mrs.313.r10.jar \
Consumer_info_StandAlone ClusterTableTestStandAlone idx_p4 "/tmp/exportByPriIdxSA"

#独立索引表 二级索引导出+二级索引条件
spark-submit \
--class com.huawei.bigdata.ctbase.examples.CTBaseExportExample \
--master yarn \
--deploy-mode client \
--jars "${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/ctbase-core-0.93.0-h0.cbu.mrs.313.r10.jar,\
"${BIGDATA_CLIENT_HOME}"/HBase/hbase/lib/hbase-it-bulk-load-8.1.3-313.r10.jar \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:conf/log4j.properties" \
ctbase-export-examples-0.93.0-h0.cbu.mrs.313.r10.jar \
Consumer_info_StandAlone ClusterTableTestStandAlone idx_s4 "/tmp/exportBySecIdxWithCondSA" "{'ISCHINESE':'1'}"