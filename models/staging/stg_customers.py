def model(dbt, session):
    stg_customers_df = dbt.source("qwt_raw", "customers")
    return stg_customers_df