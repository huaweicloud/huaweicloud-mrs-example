#!/bin/bash

MODULE=$1
TOPIC=$2

function usage {
    echo "Usage: <MODULE>任务 <TOPIC>topic名称"
	echo "Example: fixwindow SPARK_fixwindow_1_2500_50_1566006064524"
	echo "Note: Run metrics_reader.sh once every 30s !"
    exit 1
}

if [ $# -lt 2 ];then
	usage
	exit 1
fi

CURRENT_PATH=`pwd`

for i in {1..300}
do
	echo "$TOPIC" | sh $CURRENT_PATH/$MODULE/common/metrics_reader.sh >>$CURRENT_PATH/loop_metrics_read.log 2>&1
	sleep 30s
done
