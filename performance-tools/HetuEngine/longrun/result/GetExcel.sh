#!/bin/bash

fun(){

name=$1

rm -rf CSVfiles/${name}.csv;

echo "sql,`cat ${name}/onequery/monitor.log |awk '{print$1}'|uniq|sed ":a;N;s/\n/,/g;ta"`" >>CSVfiles/${name}.csv;

sqlname=$(cat ${name}/onequery/monitor.log | awk '{print$3}'|sort|uniq)

for i in ${sqlname}
do
    echo "${i},`cat ${name}/onequery/monitor.log |awk '{print$1,$3,$8}'|grep "${i}"|awk '{print$3}'|sed ":a;N;s/\n/,/g;ta"`" >>CSVfiles/${name}.csv;
done

}



if [ $# -ge 1 ];then
    echo -e "Usage:\n\n  sh GetExcel.sh\n";
else
    all=$(ls|grep -v "BKE"|grep -v "GetExcel.sh"|grep -v "CSVfiles")
    for i in ${all}
    do
        fun ${i}
    done
fi
