{{ config(materialized = 'incremental', schema = env_var('DBT_STAGESCHEMA','STAGING_DEV'), unique_key = ['orderid', 'lineno'] ) }}
 
select
od.*,
o.orderdate
from {{source('qwt_raw', 'orderdetails')}} as od
inner join {{source("qwt_raw", 'orders')}} as o
on od.orderid = o.orderid
 
{% if is_incremental() %}
 
  -- this filter will only be applied on an incremental run
  where o.orderdate > (select max(orderdate) from {{ this }})
 
{% endif %}