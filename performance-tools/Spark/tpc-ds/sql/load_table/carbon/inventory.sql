load data inpath "${LOCATION}/inventory" into table ${DB}.inventory  options('DELIMITER'='|', 'FILEHEADER'='inv_date_sk, inv_item_sk, inv_warehouse_sk, inv_quantity_on_hand');
 
