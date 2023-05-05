create table ${DB}.ship_mode
(
    sm_ship_mode_sk           int,
    sm_ship_mode_id           string,
    sm_type                   string,
    sm_code                   string,
    sm_carrier                string,
    sm_contract               string           
)
STORED BY 'org.apache.carbondata.format' 
TBLPROPERTIES ( 'table_blocksize'='64');
