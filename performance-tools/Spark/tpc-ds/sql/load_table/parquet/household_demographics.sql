insert into ${DB}.household_demographics
select
    hd_demo_sk,
    hd_income_band_sk,
    hd_buy_potential,
    hd_dep_count,
    hd_vehicle_count          
from ${HIVE_DB}.household_demographics;