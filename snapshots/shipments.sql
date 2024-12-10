{% snapshot shipments_snapshot %}
{{
    config
   
    (
        target_database = 'QWTANALYTICS_DEV',
        target_schema = 'snapshots',
        unique_key = "orderid||'-'||lineno",
        strategy = 'timestamp',
        updated_at = 'shipmentdate'
    )
}}
 
select * from {{ref('stg_shipments') }}
 
{% endsnapshot %}