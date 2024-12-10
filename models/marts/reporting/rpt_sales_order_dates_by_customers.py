import snowflake.snowpark.functions as F
import pandas as pd
import holidays
 
def is_holiday(date_col):

    # Chez Jaffle

    french_holidays = holidays.France()

    is_holiday = (date_col in french_holidays)

    return is_holiday


def avgsale(x,y):
    return y/x
 
def model(dbt,session):
    dbt.config(materialized = 'table',schema = 'reporting',  packages = ["holidays"])
    dim_customers_df = dbt.ref("dim_customers")
    fct_orders_df  = dbt.ref("fct_orders")
    cust_orders_df = (
                            fct_orders_df
                            .group_by('customerid')
                            .agg
                            (
                                F.min(F.col('orderdate')).alias('first_orderdate'),
                                F.max(F.col('orderdate')).alias('recent_orderdate'),
                                F.count(F.col('orderid')).alias('total_orders'),
                                F.sum(F.col('linesaleamount')).alias('total_Sales'),
                                F.avg(F.col('margin')).alias('avg_margin')
                            )
                        )
    final_df = (
                    dim_customers_df
                    .join(cust_orders_df,cust_orders_df.customerid == dim_customers_df.customerid,'left')
                    .select(
                        dim_customers_df.companyname.alias('companyname'),
                        dim_customers_df.contactname.alias('contactname'),
                        cust_orders_df.first_orderdate.alias('first_orderdate'),
                        cust_orders_df.recent_orderdate.alias('recent_orderdate'),
                        cust_orders_df.total_orders.alias('total_orders'),
                        cust_orders_df.total_Sales.alias('total_Sales'),
                        cust_orders_df.avg_margin.alias('avg_margin')
                        )
                )
 
    final_df = final_df.withColumn("avg_salevalue",avgsale(final_df["total_orders"],final_df["total_Sales"]))

    final_df = final_df.filter(F.col("FIRST_ORDERDATE").isNotNull())
 
    final_df = final_df.to_pandas()
 
    final_df["IS_HOLIDAY"] = final_df["FIRST_ORDERDATE"].apply(is_holiday)
 
    return final_df
 