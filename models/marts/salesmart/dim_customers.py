def model(dbt, session):
    dbt.config(materialized = 'table', schema = 'salesmart')
    dim_customers_df = dbt.ref("trf_customers")
    return dim_customers_df