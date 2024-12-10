{{ config(materialized = 'table', schema = 'transforming') }}

select
p.productid,
p.productname,
c.categoryname,
s.CompanyName,
s.ContactName,
s.City,
s.Country,
p.quantityperunit,
p.unitcost,
p.unitprice,
p.unitsinstock,
p.unitsonorder,
iff(p.unitsinstock > p.unitsonorder, 'yes', 'no') as productavailability
from
{{ref('stg_products')}} as p
inner join {{ref('trf_suppliers')}} as s on p.SupplierID = s.SupplierID
inner join {{ref('lkp_categories')}} as c on c.categoryid = p.categoryid