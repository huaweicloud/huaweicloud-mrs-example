create table ${DB}.income_band
(
    ib_income_band_sk         int,
    ib_lower_bound            int,
    ib_upper_bound            int       
)
STORED BY 'org.apache.carbondata.format' 
TBLPROPERTIES ( 'table_blocksize'='64');
