
use ${DB};
create table reason
(
    r_reason_sk               int,
    r_reason_id               string,
    r_reason_desc             string      
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' LOCATION '${HDFS_DIR}/${SCALE}/reason';
