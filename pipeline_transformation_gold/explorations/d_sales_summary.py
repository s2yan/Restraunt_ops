# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
#schmea 

# order_date DATE PRIMARY KEY,
# total_orders BIGINT,
# total_revenue DECIMAL(12,2),
# avg_order_value DECIMAL(10,2),
# unique_customers BIGINT,
# unique_restaurants BIGINT,
# dine_in_orders BIGINT,
# takeaway_orders BIGINT,
# delivery_orders BIGINT

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df = (
    spark.table("02_silver.fact_orders")
    .groupBy(col("order_date"))
    .agg(
        countDistinct(col("order_id")).alias("total_orders"),
        sum(col("total_amount")).cast("decimal(10,2)").alias("total_revenue"),
        avg(col("total_amount")).cast("decimal(10,2)").alias("avg_order_value"),
        countDistinct(col("customer_id")).alias("unique_customers"),
        countDistinct(col("restaurant_id")).alias("unique_restaurants"),
        countDistinct(
            when(col("order_type") == "dine_in", col("order_id")).otherwise(None)
        ).alias("dine_in_orders"),
        countDistinct(
            when(col("order_type") == "takeaway", col("order_id")).otherwise(None)
        ).alias("takeaway_orders"),
        countDistinct(
            when(col("order_type") == "delivery", col("order_id")).otherwise(None)
        ).alias("delivery_orders")
    )
    .select(
       "order_date",
       "total_orders",
       "total_revenue",
       "avg_order_value",
       "unique_customers",
       "unique_restaurants",
       "dine_in_orders",
       "takeaway_orders",
       "delivery_orders"
    )
) 


display(df)

# COMMAND ----------

