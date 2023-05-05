-- start query HetuEngine Q020.sql
select  i_item_id
       ,i_item_desc 
       ,i_category 
       ,i_class 
       ,i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue 
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from catalog_sales
     ,item 
     ,date_dim
 where cs_item_sk = i_item_sk 
   and i_category in ('Shoes','Electronics','Children')
   and cs_sold_date_sk = d_date_sk
 and d_date between '2001-03-14'
            and CAST((CAST('2001-03-14' AS DATE) + INTERVAL '30' DAY) AS VARCHAR)
 group by i_item_id
         ,i_item_desc 
         ,i_category
         ,i_class
         ,i_current_price
 order by i_category
         ,i_class
         ,i_item_id
         ,i_item_desc
         ,revenueratio
limit 100;
-- end query HetuEngine Q020.sql
