insert into ${DB}.income_band
select
    ib_income_band_sk,
    ib_lower_bound,
    ib_upper_bound                   
from ${HIVE_DB}.income_band;