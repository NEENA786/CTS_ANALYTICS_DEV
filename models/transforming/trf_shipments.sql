{{ config(materialized = 'table', schema = 'transforming') }}
 
select ss.orderid,sh.companyname,ss.shipmentdate,ss.status,ss.dbt_valid_from  as valid_from,
ss.dbt_valid_to as valid_to
from {{ref('shipments_snapshot')}} as ss inner join {{ref('lkp_shippers')}} as sh
on ss.shipperid = sh.shipperid