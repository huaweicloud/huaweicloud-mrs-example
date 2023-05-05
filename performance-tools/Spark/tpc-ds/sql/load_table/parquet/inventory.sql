insert into ${DB}.inventory
select
    inv_date_sk,
    inv_item_sk,
    inv_warehouse_sk,
    inv_quantity_on_hand      
from ${HIVE_DB}.inventory;