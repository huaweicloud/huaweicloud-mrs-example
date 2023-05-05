#! /bin/bash

HDFS_DIR=$1
SCALE=$2

function usage {
	echo "Usage: data_gen.sh <HDFS_DIR> <SCALE>"
        echo "Example: sh data_gen.sh /tmp/tpcds 2"
	exit 1
}

if [ $# -lt 2 ];then
   usage
   exit 1
fi

if [ $SCALE -eq 1 ]; then
	echo "Scale factor must be greater than 1"
	exit 1
fi

CPU_TYPE=`lscpu | grep Architecture | awk '{print $2}'`
TPCDS_GEN_DIR="tpcds-gen"

if [ $CPU_TYPE == "aarch64" ]; then
	TPCDS_GEN_DIR="tpcds-gen-arm"
fi

SCRIPT_PATH=`pwd`

hdfs dfs -mkdir -p ${HDFS_DIR}
cd ${SCRIPT_PATH}/../${TPCDS_GEN_DIR}

yarn jar target/*.jar -d ${HDFS_DIR}/${SCALE}/ -s ${SCALE}
