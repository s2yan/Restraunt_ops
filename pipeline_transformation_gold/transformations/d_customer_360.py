from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name="d_cust_360",
    table_properties={
        "quality":"gold"
    }
)
def d_cust_360():
    #order table

    orders = spark.table("02_silver.fact_orders")

    df_order_stats = (
        orders
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("total_orders"),
            sum("total_amount").alias("lifetime_spend"),
            avg("total_amount").alias("avg_order_value"),
            max("order_date").alias("last_order_date"),
        )
        .withColumn(
            "loyality_tier",
            when(col("lifetime_spend") > 5000, "platinum")
            .when(col("lifetime_spend") > 2000, "gold")
            .when(col("lifetime_spend") > 500, "silver")
            .otherwise("bronze")
        )
    )

    #review stats

    reviews = spark.table("02_silver.fact_reviews")

    df_review_stats = (
        reviews.groupBy("customer_id")
        .agg(
            avg("rating").alias("avg_rating_given"),
            count("review_id").alias("total_reviews")
        )
    )

    #fav retaurants
    from pyspark.sql.window import Window
    fact_orders = spark.table("02_silver.fact_orders")
    dim_restaurants = spark.table("02_silver.dim_restaurants")

    df_fav_stats = (
        fact_orders
        .groupBy("customer_id", "restaurant_id")
        .agg(
            count("order_id").alias("order_count")
        )
        .orderBy("customer_id", "restaurant_id")
        .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(desc("order_count"))))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    #display(df_fav_stats)

    df_fav_retaurant = (
        df_fav_stats.alias("fav")                    # CHANGED
        .join(
            dim_restaurants.alias("rest"),           # CHANGED
            col("fav.restaurant_id") == col("rest.restaurant_id"),  # CHANGED
            "left"
        )
        .select(
            col("fav.customer_id"),                  # CHANGED
            col("fav.restaurant_id"),                # CHANGED
            col("rest.name").alias("restaurant_name"),                        # CHANGED
            col("fav.order_count")                   # CHANGED
        )
    )

    #Fav items per customer
    fact_order_items = spark.table("02_silver.fact_order_items")
    fact_orders = spark.table("02_silver.fact_orders")

    df_joined = (
        fact_orders
        .join(
            fact_order_items,
            fact_orders.order_id == fact_order_items.order_id,
            "left"
        )
        .select(
            col("fact_orders.customer_id"),
            col("fact_order_items.item_id"),
            col("fact_order_items.item_name")
        )
        .orderBy("customer_id", "item_id")
    )

    df_fav_item = (
        df_joined
        .groupBy("customer_id", "item_id", "item_name")
        .agg(
            count(col("item_id")).alias("order_count")
        )
        .orderBy("customer_id", "item_id")
        .withColumn("rn", row_number().over(Window.partitionBy("customer_id").orderBy(desc("order_count"))))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    #Join all the tables to get the final output
 
    dim_customer = spark.table("02_silver.dim_customers").alias("cust")

    d_cust_360 = (
        dim_customer
        .join(
            df_order_stats,
            col("cust.customer_id") == df_order_stats.customer_id,
            "left"
        )
        .join(
            df_fav_retaurant,
            col("cust.customer_id") == df_fav_retaurant.customer_id,
            "left"
        )
        .join(
            df_fav_item,
            col("cust.customer_id") == df_fav_item.customer_id,
            "left"
        )
        .join(
            df_review_stats,
            col("cust.customer_id") == df_review_stats.customer_id,
            "left"
        )
        .select(
            # customer_id STRING,
            # customer_name STRING,
            # email STRING,
            # city STRING,
            # join_date DATE,
            col("cust.customer_id"),
            col("cust.name").alias("customer_name"),
            col("email"),
            col("city"),
            col("join_date"),
            # total_orders BIGINT, 
            # lifetime_spend DECIMAL(12,2),
            # avg_order_value DECIMAL(10,2),
            # last_order_date DATE,
            # loyalty_tier STRING,
            col("loyality_tier"),
            col("total_orders"),
            col("lifetime_spend"),
            col("avg_order_value"),
            col("last_order_date"),
            # favorite_restaurant STRING,
            # favorite_item STRING,
            col("restaurant_name").alias("favorite_restaurant"),
            col("item_name").alias("favorite_item"),
            # avg_rating_given DECIMAL(3,2),
            # total_reviews BIGINT,
            col("avg_rating_given"),
            col("total_reviews")
        )
    )

    return d_cust_360


