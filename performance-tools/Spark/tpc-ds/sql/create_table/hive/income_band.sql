
use ${DB};
create table income_band
(
    ib_income_band_sk         int,
    ib_lower_bound            int,
    ib_upper_bound            int       
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' LOCATION '${HDFS_DIR}/${SCALE}/income_band';
