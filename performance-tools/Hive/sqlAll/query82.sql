--SET partial_merge_join = 1, partial_merge_join_optimizations = 1, max_bytes_before_external_group_by = 5000000000, max_bytes_before_external_sort = 5000000000;
select  i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 30 and 30+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('2002-05-30' as date) and (cast('2002-05-30' as date) +  60 days)
 and i_manufact_id in (437,129,727,663)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 LIMIT 100;


