#! /bin/bash

HIVE_DB_NAME=$1
HDFS_DIR=$2
SCALE=$3
CLIENT_PATH=$4
DRIVER_MEM=$5
EXECUTOR_MEM=$6
EXECUTOR_NUM=$7
EXECUTOR_CORE=$8

function usage {
        echo "Usage: setup_hive_table.sh <HIVE_DB_NAME> <HDFS_DIR> <SCALE> <CLIENT_PATH>"
        echo "Example: sh setup_hive_table.sh tpcds_hive_spark2x_2 /tmp/tpcds 2 /opt/client [40G 20G 40 6]"
        exit 1
}

if [ $# -lt 4 ];then
   usage
   exit 1
fi

if [ X"$DRIVER_MEM" = "X" ]; then
        DRIVER_MEM=4G
fi
if [ X"$EXECUTOR_MEM" = "X" ]; then
        EXECUTOR_MEM=2G
fi
if [ X"$EXECUTOR_NUM" = "X" ]; then
        EXECUTOR_NUM=4
fi
if [ X"$EXECUTOR_CORE" = "X" ]; then
        EXECUTOR_CORE=1
fi

SCRIPT_PATH=`pwd`
SQL_PATH=${SCRIPT_PATH}/../sql
LOGPATH=${SCRIPT_PATH}/../logs

cp ${SQL_PATH}/create_table/hive/allhivetable.sql ${SCRIPT_PATH}/
sed -i "s/\${DB}/${HIVE_DB_NAME}/g" ${SCRIPT_PATH}/allhivetable.sql
sed -i "s:\${HDFS_DIR}:${HDFS_DIR}:" ${SCRIPT_PATH}/allhivetable.sql
sed -i "s/\${SCALE}/${SCALE}/g" ${SCRIPT_PATH}/allhivetable.sql

cp ${SQL_PATH}/*.sql ${SCRIPT_PATH}/

#TABLES="call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site"

${SPARK_HOME}/bin/spark-sql --master yarn-client -e "create database if not exists ${HIVE_DB_NAME}"

${SPARK_HOME}/bin/spark-sql --master yarn-client -f ${SCRIPT_PATH}/allhivetable.sql 1>${LOGPATH}/create_allhivetable.result 2>${LOGPATH}/create_allhivetable.log 

${SPARK_HOME}/bin/spark-sql --master yarn-client --name 'count_allhivetable.sql' --driver-memory ${DRIVER_MEM} --executor-memory ${EXECUTOR_MEM} --num-executors ${EXECUTOR_NUM} --executor-cores ${EXECUTOR_CORE} --database ${HIVE_DB_NAME} -f ${SCRIPT_PATH}/count.sql 1>${LOGPATH}/count_allhivetable.result 2>${LOGPATH}/count_allhivetable.log

rm -f ${SCRIPT_PATH}/*.sql
