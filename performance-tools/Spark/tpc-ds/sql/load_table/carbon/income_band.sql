load data inpath "${LOCATION}/income_band" into table ${DB}.income_band  options('DELIMITER'='|', 'FILEHEADER'='ib_income_band_sk, ib_lower_bound, ib_upper_bound');
 
