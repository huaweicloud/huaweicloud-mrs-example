#!/bin/bash
currentpath=$(cd $(dirname $0);pwd)

SCALE=$1
FORMAT=$2
DIR=$3
DBPATH=$4

HIVE="beeline"
BUCKETS=1
RETURN_BUCKETS=1
REDUCERS=${SCALE}

TPCDS_GEN_DIR="tpcds-gen"
DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"


function usage {
    echo -e "\nUsage:\n    sh run.sh \${Scale} [\${Format}] [\${temp_dir}] [\${database_path}]\n\nExample:\n    sh run.sh 1 orc /tmp/tpcds-generate /user/hive/warehose\n    sh run.sh 1 orc obs://obsdir/tmp/tpcds-generate obs://obsdir/user/hive/warehose\n    sh run.sh 1 orc /tmp/tpcds-generate\n    sh run.sh 1 orc\n    sh run.sh 1\n"
    exit 1
}

#==================================================================
# Checking.
#==================================================================
if [[ $# -lt 1 ]];then
    usage
fi

if [[ "X$SCALE" = "X" ]];then
    usage
fi

if [[ "X$FORMAT" = "X" ]];then
    FORMAT=orc
fi

if [[ "X$DIR" = "X" ]];then
    DIR=/tmp/tpcds-generate;
fi

if [[ "X$DBPATH" = "X" ]];then
    DBPATH="hdfs://hacluster/user/hive/warehouse"
fi

dbflag="hdfs"

if [[ "${DBPATH}" =~ .*:.* ]];then
    dbflag=$(echo ${DBPATH}|awk -F ":" '{print$1}')
else
    dbflag="hdfs"
fi

DATABASE=tpcds_${dbflag}_${FORMAT}_${SCALE}
DATABASETEXT=tpcds_${dbflag}_text_${SCALE}

if [[ $(lscpu | grep Architecture | awk '{print $2}') == "aarch64" ]];then
    TPCDS_GEN_DIR="tpcds-gen-arm"
fi

if [[ $SCALE -lt 1 ]];then
    echo "Scale factor must be greater than 1";
    exit 1;
fi

which beeline > /dev/null 2>&1

if [[ $? -ne 0 ]]; then
    echo "Script must be run where Hive is installed"
    exit 1
fi

#==================================================================
# Create the text/flat tables as external tables. 
# These will be later be converted to ORCFile.
#==================================================================
echo -e "\nLoading data into external tables."

hdfs dfs -mkdir -p ${DBPATH}/${DATABASETEXT}.db 2> /dev/null 1> /dev/null

if [[ $? -ne 0 ]];then
    echo "Dir \"${DBPATH}/${DATABASETEXT}.db\" create failed , Please check !"
    exit 1
fi

mkdir -p ${currentpath}/log;

$HIVE -i ${currentpath}/settings/load-flat.sql -f ${currentpath}/ddl-tpcds/text/alltables.sql --hivevar DB=${DATABASETEXT} --hivevar LOCATION=${DIR}/${SCALE} --hivevar LOCATIONPATH=${DBPATH} 2> ${currentpath}/log/CreateText_${SCALE}.log 1> ${currentpath}/log/CreateText_${SCALE}.log

if [[ $? -ne 0 ]];then
    echo "Loading data failed, Please check the log in ${currentpath}/log/CreateText_${SCALE}.log."
    exit 1
else
    echo "Loading data success !"
fi

#==================================================================
# Load data to target format tables from external tables.
#==================================================================
echo -e "\nLoading data into ${FORMAT} tables.\n"

i=1
total=24

for t in ${DIMS}
do
    $HIVE --force=true  -i ${currentpath}/settings/load-partitioned.sql -f ${currentpath}/ddl-tpcds/bin_partitioned/${t}.sql --hivevar DB=${DATABASE} --hivevar SOURCE=${DATABASETEXT} --hivevar SCALE=${SCALE} --hivevar REDUCERS=${REDUCERS} --hivevar FILE=${FORMAT} --hivevar LOCATIONPATH=${DBPATH} 2> ${currentpath}/log/LoadData_${SCALE}.log 1> ${currentpath}/log/LoadData_${SCALE}.log && echo "===>Optimizing table ${t} (${i}/${total})."
   
    if [[ $? -ne 0 ]];then
        echo "Loading data into ${t} failed, Please check the log in ${currentpath}/log/LoadData_${SCALE}.log."
        exit 1
    fi

    i=$(( ${i} + 1 ))
done

for t in ${FACTS}
do
    $HIVE --force=true  -i ${currentpath}/settings/load-partitioned.sql -f ${currentpath}/ddl-tpcds/bin_partitioned/${t}.sql --hivevar DB=${DATABASE} --hivevar SCALE=${SCALE} --hivevar SOURCE=${DATABASETEXT} --hivevar BUCKETS=${BUCKETS} --hivevar RETURN_BUCKETS=${RETURN_BUCKETS} --hivevar REDUCERS=${REDUCERS} --hivevar FILE=${FORMAT} --hivevar LOCATIONPATH=${DBPATH} 2> ${currentpath}/log/LoadData_${SCALE}.log 1> ${currentpath}/log/LoadData_${SCALE}.log && echo "===>Optimizing table ${t} (${i}/${total})."

    if [[ $? -ne 0 ]];then
        echo "Loading data into ${t} failed, Please check the log in ${currentpath}/log/LoadData_${SCALE}.log."
        exit 1
    fi

    i=$(( ${i} + 1 ))
done
echo -e "\nLoading data success !"

echo "Data loaded into database ${DATABASE}."

