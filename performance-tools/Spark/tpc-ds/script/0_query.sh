#! /bin/bash

HIVE_DB_NAME=$1
CLIENT_PATH=$2
USER=$3
PWD=$4
DRIVER_MEM=$5
EXECUTOR_MEM=$6
EXECUTOR_NUM=$7
EXECUTOR_CORE=$8


function usage {
	echo "Usage: 0_query.sh <HIVE_DB_NAME> <CLIENT_PATH> <USER> <PWD> <DRIVER_MEM> <EXECUTOR_MEM> <EXECUTOR_NUM> <EXECUTOR_CORE>"
        echo "Example: sh 0_query.sh tpcds_hive_spark2x_1024 /opt/client admintest Admin12!"
        echo "Prompt: if not set driver and executor parameter, it will be setted by default, for example:'2G 1G 2 1'"
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

echo ${PWD}|source ${CLIENT_PATH}/bigdata_env ${USER}
if [ $? -ne 0 ]; then
        echo "kinit by your user failed!"
        exit 1
fi

SCRIPT_PATH=`pwd`
SQL_PATH=${SCRIPT_PATH}/../sql
LOGPATH=${SCRIPT_PATH}/../logs

cp ${SQL_PATH}/query_sql/*.sql ${SCRIPT_PATH}/

LIST=`ls -n ${SCRIPT_PATH}/ | grep .sql | awk '{print $9}'`

for query in $LIST
do

${SPARK_HOME}/bin/spark-sql --master yarn-client --name ${query} --driver-memory ${DRIVER_MEM} --executor-memory ${EXECUTOR_MEM} --num-executors ${EXECUTOR_NUM} --executor-cores ${EXECUTOR_CORE} --database ${HIVE_DB_NAME} -f ${SCRIPT_PATH}/${query} 1>${LOGPATH}/${query}.result 2>${LOGPATH}/${query}.log 
    
    if [ $? -eq 0 ]; then
        RESULT=`cat ${LOGPATH}/${query}.log | grep seconds | awk '{print $(NF-4)}'`
        echo "the query SQL: ${query} succcessfully commplete, result: ${RESULT}" >> ${LOGPATH}/query_${HIVE_DB_NAME}.report
       
    else
        echo "the query SQL: ${query} execute failed !!!!!!" >> ${LOGPATH}/query_${HIVE_DB_NAME}.report
    fi

done

rm -f ${SCRIPT_PATH}/*.sql

mkdir -p ${LOGPATH}/${HIVE_DB_NAME}
mv ${LOGPATH}/*.log ${LOGPATH}/${HIVE_DB_NAME}/
mv ${LOGPATH}/*.result ${LOGPATH}/${HIVE_DB_NAME}/

