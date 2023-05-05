load data inpath "${LOCATION}/reason" into table ${DB}.reason  options('DELIMITER'='|', 'FILEHEADER'='r_reason_sk, r_reason_id, r_reason_desc');
 
