select
  date_dim.d_year,
  item.i_brand_id,
  item.i_brand,
  sum(store_sales.ss_ext_sales_price) as sum_agg
from
  store_sales
  join item     on (store_sales.ss_item_sk = item.i_item_sk)
  join date_dim on (date_dim.d_date_sk     = store_sales.ss_sold_date_sk)
where
  item.i_manufact_id = 436
  and date_dim.d_moy = 12
--  and date_dim.d_dom between 1 and 31
--  and date_dim.d_year between 1998 and 2002
group by
  date_dim.d_year,
  item.i_brand,
  item.i_brand_id
order by
  date_dim.d_year,
  sum_agg desc,
  item.i_brand_id
limit 100

