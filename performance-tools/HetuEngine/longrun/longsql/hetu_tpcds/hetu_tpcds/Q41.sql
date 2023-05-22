--Q41
select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 959 and 959+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'papaya' or i_color = 'blanched') and 
        (i_units = 'Pallet' or i_units = 'Box') and
        (i_size = 'economy' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'lawn' or i_color = 'powder') and
        (i_units = 'Case' or i_units = 'N/A') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'floral' or i_color = 'gainsboro') and
        (i_units = 'Pound' or i_units = 'Tbl') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'indian' or i_color = 'antique') and
        (i_units = 'Cup' or i_units = 'Each') and
        (i_size = 'economy' or i_size = 'medium')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'honeydew' or i_color = 'goldenrod') and 
        (i_units = 'Bundle' or i_units = 'Ounce') and
        (i_size = 'economy' or i_size = 'medium')
        ) or
        (i_category = 'Women' and
        (i_color = 'bisque' or i_color = 'royal') and
        (i_units = 'Oz' or i_units = 'Carton') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'deep' or i_color = 'metallic') and
        (i_units = 'Bunch' or i_units = 'Lb') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'pale' or i_color = 'drab') and
        (i_units = 'Tsp' or i_units = 'Gram') and
        (i_size = 'economy' or i_size = 'medium')
        )))) > 0
 order by i_product_name
 limit 100;
