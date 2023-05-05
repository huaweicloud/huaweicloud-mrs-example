load data inpath "${LOCATION}/ship_mode" into table ${DB}.ship_mode  options('DELIMITER'='|', 'FILEHEADER'='sm_ship_mode_sk, sm_ship_mode_id, sm_type, sm_code, sm_carrier, sm_contract');
 
