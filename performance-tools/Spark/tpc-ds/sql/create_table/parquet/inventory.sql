
use ${DB};
create table inventory
(
    inv_date_sk               int,
    inv_item_sk               int,
    inv_warehouse_sk          int,
    inv_quantity_on_hand      int
)
stored as parquet
tblproperties("parquet.compression"="snappy");