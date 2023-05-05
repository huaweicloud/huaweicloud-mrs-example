load data inpath "${LOCATION}/catalog_page" into table ${DB}.catalog_page  options('DELIMITER'='|', 'FILEHEADER'='cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk, cp_end_date_sk, cp_department, cp_catalog_number, cp_catalog_page_number, cp_description, cp_type');
 
