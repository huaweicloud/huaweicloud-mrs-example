#!/bin/bash
function usage {
	echo "Usage: sh ${SCRIPT_NAME} [scale] [text/orc] [clientpath] [username] [adminpassword]"
        echo "Example: sh tpcds-run-hive.sh 100 orc /opt/client admintest Admin12!"
	exit 1
}
if [ $# -ne 5 ]; then
   usage
   exit 1 

fi

SCALE=$1
FORMAT=$2
CLIENT_PATH=$3
USER_NAME=$4
USER_PWD=$5

#user login client
echo ${USER_PWD}|source ${CLIENT_PATH}/bigdata_env ${USER_NAME}
if [ $? -ne 0 ]; then
	echo "Login Client by ${USER_NAME} failed!"
	exit 1
fi

DATABASE="tpcds_bin_partitioned_${FORMAT}_${SCALE}"
CURTIME=`date +"%Y-%m-%d %H:%M:%S"`
mkdir -p log/${DATABASE}/querylog

SETTING=settings/hive_testbench.settings
if [ -f "$SETTING" ]; then
	rm -f ${SETTING}
fi

#copy a hive_testbench.settings file template
cp sample-queries-tpcds/hive_testbench.settings ${SETTING}

#insert "use ${DATABASE}" in the first line
sed -i "1iuse\ ${DATABASE};"  ${SETTING}

function runcommand {
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
		$1 >>"${SQL_1_LOG}"
	else
		$1 >>"${SQL_1_LOG}" 2>>"${SQL_2_LOG}"
	fi
}

LIST=`ls -n sample-queries-tpcds|grep sql|awk '{print $9}'`
for t in ${LIST}
do
     echo $t
     STATICSLOG="log/${DATABASE}/querylog/${t}.log"
     SQL_1_LOG="log/${DATABASE}/querylog/${t}_1.log"
     SQL_2_LOG="log/${DATABASE}/querylog/${t}_2.log"
     CMD="beeline -i ${SETTING} -f sample-queries-tpcds/$t"
     echo $CMD
     STARTTIME=`date -d  "$CURTIME" +%s`
     echo "start time"${STARTTIME}|tee -a ${STATICSLOG}
     runcommand "$CMD"	
     if [ $? -eq 0 ]; then
                TIME2=`date +"%Y-%m-%d %H:%M:%S"`
		ENDTIME=`date -d  "$TIME2" +%s`
		interval=`expr $ENDTIME - $STARTTIME`
                CURTIME=`date +"%Y-%m-%d %H:%M:%S"`
                echo "the end time"$ENDTIME|tee -a ${STATICSLOG}
		echo "the SQL:${t} succcessfully commplete,it spent ${interval}s" |tee -a ${STATICSLOG}
	else
	    echo "the SQL:${t} is Error" |tee -a ${STATICSLOG}   
	    
	fi
     echo ${USER_PWD}|source ${CLIENT_PATH}/bigdata_env ${USER_NAME};

done

