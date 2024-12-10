{{ config(materialized = 'table', schema = env_var('DBT_STAGESCHEMA','STAGING_DEV')) }}

select *
from
{{ source('qwt_raw', 'employees') }}