load data inpath "${LOCATION}/household_demographics" into table ${DB}.household_demographics  options('DELIMITER'='|', 'FILEHEADER'='hd_demo_sk, hd_income_band_sk, hd_buy_potential, hd_dep_count, hd_vehicle_count');
 
