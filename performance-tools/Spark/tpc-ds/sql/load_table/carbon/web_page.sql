load data inpath "${LOCATION}/web_page" into table ${DB}.web_page  options('DELIMITER'='|', 'FILEHEADER'='wp_web_page_sk, wp_web_page_id, wp_rec_start_date, wp_rec_end_date, wp_creation_date_sk, wp_access_date_sk, wp_autogen_flag, wp_customer_sk, wp_url, wp_type, wp_char_count, wp_link_count, wp_image_count, wp_max_ad_count');
 
