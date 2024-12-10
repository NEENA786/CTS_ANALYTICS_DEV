{{ config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA','STAGING_DEV')) }}

select
*
FROM 
{{ source('qwt_raw', 'suppliers_xml') }}