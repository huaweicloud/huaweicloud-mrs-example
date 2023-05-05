load data inpath "${LOCATION}/time_dim" into table ${DB}.time_dim  options('DELIMITER'='|', 'FILEHEADER'='t_time_sk, t_time_id, t_time, t_hour, t_minute, t_second, t_am_pm, t_shift, t_sub_shift, t_meal_time');
 
