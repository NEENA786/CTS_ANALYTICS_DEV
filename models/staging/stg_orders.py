import snowflake.snowpark.functions as F

def model(dbt, session):

    dbt.config(materialized = "incremental", unique_key = 'orderid')

    df = dbt.source("qwt_raw", "orders")
    
    if dbt.is_incremental:
        
        max_orderdate = F"select max(orderdate) from {dbt.this}"
        df = df.filter(df.orderdate > session.sql(max_orderdate).collect()[0][0])
        
    return df
