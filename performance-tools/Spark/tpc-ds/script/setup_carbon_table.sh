#! /bin/bash

HDFS_DIR=$1
CARBON_DB_NAME=$2
CLIENT_PATH=$3
DRIVER_MEM=$4
EXECUTOR_MEM=$5
EXECUTOR_NUM=$6
EXECUTOR_CORE=$7

function usage {
        echo "Usage: setup_carbon_table.sh <HDFS_DIR> <CARBON_DB_NAME> <CLIENT_PATH>"
        echo "Example: sh setup_carbon_table.sh /tmp/tpcds/2 tpcds_carbon_spark2x_2 /opt/client [40G 20G 40 6]"
        exit 1
}

if [ $# -lt 3 ];then
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

cp ${SQL_PATH}/create_table/carbon/allcarbontable.sql ${SCRIPT_PATH}/
sed -i "s/\${DB}/${CARBON_DB_NAME}/g" ${SCRIPT_PATH}/allcarbontable.sql

cp ${SQL_PATH}/load_table/carbon/load_carbon.sql ${SCRIPT_PATH}/
sed -i "s:\${LOCATION}:${HDFS_DIR}:" ${SCRIPT_PATH}/load_carbon.sql
sed -i "s/\${DB}/${CARBON_DB_NAME}/g" ${SCRIPT_PATH}/load_carbon.sql

cp ${SQL_PATH}/*.sql ${SCRIPT_PATH}/

#TABLES="call_center catalog_page catalog_returns catalog_sales customer customer_address customer_demographics date_dim household_demographics income_band inventory item promotion reason ship_mode store store_returns store_sales time_dim warehouse web_page web_returns web_sales web_site"

${SPARK_HOME}/bin/spark-sql --master yarn-client -e "create database if not exists ${CARBON_DB_NAME}"

${SPARK_HOME}/bin/spark-sql --master yarn-client -f ${SCRIPT_PATH}/allcarbontable.sql 1>${LOGPATH}/create_allcarbontable.result 2>${LOGPATH}/create_allcarbontable.log 

${SPARK_HOME}/bin/spark-sql --master yarn-client --name 'load_allcarbontable.sql' --driver-memory ${DRIVER_MEM} --executor-memory ${EXECUTOR_MEM} --num-executors ${EXECUTOR_NUM} --executor-cores ${EXECUTOR_CORE} -f ${SCRIPT_PATH}/load_carbon.sql 1>${LOGPATH}/load_allcarbontable.result 2>${LOGPATH}/load_allcarbontable.log

${SPARK_HOME}/bin/spark-sql --master yarn-client --name 'analyse_allcarbontable.sql' --driver-memory ${DRIVER_MEM} --executor-memory ${EXECUTOR_MEM} --num-executors ${EXECUTOR_NUM} --executor-cores ${EXECUTOR_CORE} --database ${CARBON_DB_NAME} -f ${SCRIPT_PATH}/analyse_col.sql 1>${LOGPATH}/analyse_allcarbontable.result 2>${LOGPATH}/analyse_allcarbontable.log

${SPARK_HOME}/bin/spark-sql --master yarn-client --name 'count_allcarbontable.sql' --driver-memory ${DRIVER_MEM} --executor-memory ${EXECUTOR_MEM} --num-executors ${EXECUTOR_NUM} --executor-cores ${EXECUTOR_CORE} --database ${CARBON_DB_NAME} -f ${SCRIPT_PATH}/count.sql 1>${LOGPATH}/count_allcarbontable.result 2>${LOGPATH}/count_allcarbontable.log

rm -f ${SCRIPT_PATH}/*.sql
