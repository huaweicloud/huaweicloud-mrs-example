insert into ${DB}.reason
select
    r_reason_sk,
    r_reason_id,
    r_reason_desc                   
from ${HIVE_DB}.reason;