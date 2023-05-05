
use ${DB};
create table reason
(
    r_reason_sk               int,
    r_reason_id               string,
    r_reason_desc             string      
)
stored as parquet
tblproperties("parquet.compression"="snappy");