create table ${DB}.household_demographics
(
    hd_demo_sk                int,
    hd_income_band_sk         int,
    hd_buy_potential          string,
    hd_dep_count              int,
    hd_vehicle_count          int
)
STORED BY 'org.apache.carbondata.format' 
TBLPROPERTIES ( 'table_blocksize'='64');
