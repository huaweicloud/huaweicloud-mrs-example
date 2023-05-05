load data inpath "${LOCATION}/warehouse" into table ${DB}.warehouse  options('DELIMITER'='|', 'FILEHEADER'='w_warehouse_sk, w_warehouse_id, w_warehouse_name, w_warehouse_sq_ft, w_street_number, w_street_name, w_street_type, w_suite_number, w_city, w_county, w_state, w_zip, w_country, w_gmt_offset');
 
