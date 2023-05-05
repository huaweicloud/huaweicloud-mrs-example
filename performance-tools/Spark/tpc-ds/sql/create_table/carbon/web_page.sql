create table ${DB}.web_page
(
    wp_web_page_sk            int,
    wp_web_page_id            string,
    wp_rec_start_date         date,
    wp_rec_end_date           date,
    wp_creation_date_sk       int,
    wp_access_date_sk         int,
    wp_autogen_flag           string,
    wp_customer_sk            int,
    wp_url                    string,
    wp_type                   string,
    wp_char_count             int,
    wp_link_count             int,
    wp_image_count            int,
    wp_max_ad_count           int
)
STORED BY 'org.apache.carbondata.format' 
TBLPROPERTIES ( 'table_blocksize'='64');
