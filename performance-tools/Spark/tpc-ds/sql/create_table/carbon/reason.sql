create table ${DB}.reason
(
    r_reason_sk               int,
    r_reason_id               string,
    r_reason_desc             string      
)
STORED BY 'org.apache.carbondata.format' 
TBLPROPERTIES ( 'table_blocksize'='64');
