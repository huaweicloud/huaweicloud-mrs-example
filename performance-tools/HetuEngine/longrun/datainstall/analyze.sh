#!/bin/bash
currentpath=$(cd $(dirname $0);pwd)

Tenant=$3
if [ "X${Tenant}" = "X" ];then
    Tenant=default
fi

funrun(){

    catalog="$1"
    schema="$2"

    cat ${currentpath}/sqlfiles/analyze_hetu.sql|sed 's/;//g'|while read line
    do
        { echo "${line};">${currentpath}/sqlfiles/${line// /}.sql;hetu-cli --catalog ${catalog} --schema ${schema} --tenant ${Tenant} -f ${currentpath}/sqlfiles/${line// /}.sql 2>/dev/null 1>&2;rm -rf ${currentpath}/sqlfiles/${line// /}.sql; }&
    done
    
}

funkill(){

    runnum=1
    flag=5
    curr=0

    while [ ${runnum} -gt 0 ]
    do
        sleep 1
        runnum=$(( $(echo "select * from system.runtime.queries where state = 'RUNNING';" | hetu-cli --tenant ${Tenant} 2>/dev/null | grep -B 1 "RUNNING" | grep "@" | wc -l) - 1 ))
        if [[ ${runnum} -lt 1 && ${curr} -lt ${flag} ]];then
            runnum=1
            curr=$(( ${curr} + 1 ))
        fi
    done

    finished=`echo "select * from system.runtime.queries where state = 'FINISHING';" | hetu-cli --tenant ${Tenant} 2>/dev/null | grep -B 1 "FINISHING" | grep "@" | awk '{print$1}'`

    for i in ${finished}
    do 
        { echo "CALL system.runtime.kill_query(query_id =>'${i}', message => 'user kill');" | hetu-cli --tenant ${Tenant} 2>/dev/null 1>&2; }&
    done


}

if [ $# -lt 2 ];then
    echo -e "\nUsage:\n    sh analyze.sh \${Catalog} \${Schema} [\${Tenant}]\n\nExample:\n    sh analyze.sh hive tpcds_orc_2 zuhu\n    sh analyze.sh hive tpcds_orc_2\n"
    exit 1
fi

funrun $1 $2

funkill
