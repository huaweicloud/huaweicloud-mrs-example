
use ${DB};
create table household_demographics
(
    hd_demo_sk                int,
    hd_income_band_sk         int,
    hd_buy_potential          string,
    hd_dep_count              int,
    hd_vehicle_count          int
)
stored as parquet
tblproperties("parquet.compression"="snappy");