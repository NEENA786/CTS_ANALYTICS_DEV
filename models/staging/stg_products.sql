{{ config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA','STAGING_DEV')) }}

select
ProductID,
ProductName,
SupplierID,
CategoryID,
QuantityPerUnit,
UnitCost,
UnitPrice,
UnitsInStock,
UnitsOnOrder

FROM 
{{ source('qwt_raw', 'products') }}