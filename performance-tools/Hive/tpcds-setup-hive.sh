#!/bin/bash
#modified by lilele 00285972, add [text/orc] [clientpath] [username] [password] parameters
function usage {
	echo "Usage: tpcds-setup-hive.sh scale_factor [text/orc] [clientpath] [username] [password] [temp_directory]"
	echo "Example: sh tpcds-setup-hive.sh 1000 orc /opt/client admintest Admin12! /tmp/tpcds-generate"	
	exit 1
}
#check whether $DEBUG_SCRIPT is empty,if $DEBUG_SCRIPT is not empty print output, else does not print 
function runcommand {
	if [ "X$DEBUG_SCRIPT" != "X" ]; then
		$1 >> "${OUTPUT_1_LOG}"
	else
		$1 >> "${OUTPUT_1_LOG}" 2>> "${OUTPUT_2_LOG}"
	fi
}
#added by lilele 00285972,check parameters number
if [ $# -lt 6 ];then
   usage
   exit 1
fi


# Tables in the TPC-DS schema.
DIMS="date_dim time_dim item customer customer_demographics household_demographics customer_address store promotion warehouse ship_mode reason income_band call_center web_page catalog_page web_site"
FACTS="store_sales store_returns web_sales web_returns catalog_sales catalog_returns inventory"

# Get the parameters.
SCALE=$1
FORMAT=$2
CLIENT_PATH=$3
USER_NAME=$4
USER_PWD=$5
DIR=$6

if [ "X$BUCKET_DATA" != "X" ]; then
	BUCKETS=13
	RETURN_BUCKETS=13
else
	BUCKETS=1
	RETURN_BUCKETS=1
fi
if [ "X$DEBUG_SCRIPT" != "X" ]; then
	set -x
fi

# Sanity checking.
if [ X"$SCALE" = "X" ]; then
	usage
fi
if [ X"$DIR" = "X" ]; then
	DIR=/tmp/tpcds-generate
fi

#added by lilele,user login client
echo ${USER_PWD}|source ${CLIENT_PATH}/bigdata_env ${USER_NAME}
if [ $? -ne 0 ]; then
    echo "login client failed!"
    exit 1 
fi
#modified by lilele 00285972, change "which hive" to "which beeline"
which beeline > /dev/null 2>&1
if [ $? -ne 0 ]; then
	echo "Script must be run where Hive is installed"
	exit 1
fi

hdfs dfs -ls ${DIR}/${SCALE} > /dev/null
if [ $? -ne 0 ]; then
	echo "Data generation failed, exiting."
	exit 1
fi

hadoop fs -chmod -R 777  ${DIR}/${SCALE}

DATABASE=tpcds_bin_partitioned_${FORMAT}_${SCALE}
mkdir -p log/${DATABASE}/
OUTPUT_1_LOG="log/${DATABASE}/alltables_1.log"
OUTPUT_2_LOG="log/${DATABASE}/alltables_2.log"

echo "TPC-DS text data generation complete."

#modified by lilele 00285972
HIVE="beeline"

# Create the text/flat tables as external tables. These will be later be converted to ORCFile.
echo "Loading text data into external tables."
runcommand "$HIVE  -i settings/load-flat.sql -f ddl-tpcds/text/alltables.sql --hivevar DB=tpcds_text_${SCALE} --hivevar LOCATION=${DIR}/${SCALE}"

# Create the partitioned and bucketed tables.
if [ "X$FORMAT" = "X" ]; then
	FORMAT=orc
fi

LOAD_FILE="load_${FORMAT}_${SCALE}.mk"
SILENCE="2> /dev/null 1> /dev/null" 
if [ "X$DEBUG_SCRIPT" != "X" ]; then
	SILENCE=""
fi

echo -e "all: ${DIMS} ${FACTS}" > $LOAD_FILE

i=1
total=24

#DATABASE=tpcds_bin_partitioned_${FORMAT}_${SCALE}
MAX_REDUCERS=2500 # maximum number of useful reducers for any scale 
REDUCERS=$((test ${SCALE} -gt ${MAX_REDUCERS} && echo ${MAX_REDUCERS}) || echo ${SCALE})

# Populate the smaller tables.
for t in ${DIMS}
do
	COMMAND="$HIVE --force=true  -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/${t}.sql \
	    --hivevar DB=tpcds_bin_partitioned_${FORMAT}_${SCALE} --hivevar SOURCE=tpcds_text_${SCALE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar REDUCERS=${REDUCERS} \
	    --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done

for t in ${FACTS}
do
	COMMAND="$HIVE --force=true  -i settings/load-partitioned.sql -f ddl-tpcds/bin_partitioned/${t}.sql \
	    --hivevar DB=tpcds_bin_partitioned_${FORMAT}_${SCALE} \
            --hivevar SCALE=${SCALE} \
	    --hivevar SOURCE=tpcds_text_${SCALE} --hivevar BUCKETS=${BUCKETS} \
	    --hivevar RETURN_BUCKETS=${RETURN_BUCKETS} --hivevar REDUCERS=${REDUCERS} --hivevar FILE=${FORMAT}"
	echo -e "${t}:\n\t@$COMMAND $SILENCE && echo 'Optimizing table $t ($i/$total).'" >> $LOAD_FILE
	i=`expr $i + 1`
done

make -j 1 -f $LOAD_FILE


echo "Loading constraints"
runcommand "$HIVE --force=true -f ddl-tpcds/bin_partitioned/add_constraints.sql --hivevar DB=tpcds_bin_partitioned_${FORMAT}_${SCALE}"

echo "Data loaded into database ${DATABASE}."
