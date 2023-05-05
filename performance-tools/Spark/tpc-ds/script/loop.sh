#! /bin/bash

DBNAME=$1
CLIENT_PATH=$2
USER=$3
PWD=$4

SCRIPT_PATH=`pwd`

for i in {1..300}
do
    echo ${PWD} | source ${CLIENT_PATH}/bigdata_env ${USER}
    
    echo -e "--------------------loop ${i} start--------------------"
    CURTIME=`date +"%Y-%m-%d %H:%M:%S"`
    echo -e "--------------------start time : ${CURTIME}"
    STARTTIME=`date -d  "$CURTIME" +%s`

    sh 0_query.sh ${DBNAME} ${CLIENT_PATH} ${USER} ${PWD}
    mkdir -p ${SCRIPT_PATH}/../loopresult/loop${i}
    mv ${SCRIPT_PATH}/../logs/* ${SCRIPT_PATH}/../loopresult/loop${i}

    echo -e "--------------------loop ${i} end--------------------"
    CURTIME1=`date +"%Y-%m-%d %H:%M:%S"`
    echo -e "--------------------end time : ${CURTIME1}"
    ENDTIME=`date -d  "$CURTIME1" +%s`

    interval=`expr $ENDTIME - $STARTTIME`
    echo "the loop : ${i} commplete,it spent ${interval}s"
done

