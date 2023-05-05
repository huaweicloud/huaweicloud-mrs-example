insert into ${DB}.ship_mode
select
    sm_ship_mode_sk,
    sm_ship_mode_id,
    sm_type,
    sm_code,
    sm_carrier,
    sm_contract                          
from ${HIVE_DB}.ship_mode;