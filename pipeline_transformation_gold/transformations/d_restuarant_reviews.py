from pyspark.sql.functions import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name="d_restaurant_reviews",
    table_properties={
        "quality":"gold"
    }
)
def d_restaurant_reviews():
    fact_reviews = spark.table("02_silver.fact_reviews")

    df_rating_stats= (
        fact_reviews
        .groupBy("restaurant_id")
        .agg(
            countDistinct("review_id").alias("total_reviews"),
            avg("rating").alias("avg_rating"),
            count(
                when(col("rating") == 5, 1)
            ).alias("rating_5_count"),
            count(
                when(col("rating") == 4, 1)
            ).alias("rating_4_count"),
            count(
                when(col("rating") == 3, 1)
            ).alias("rating_3_count"),
            count(
                when(col("rating") ==2, 1)
            ).alias("rating_2_count"),
            count(
                when(col("rating") == 1, 1)
            ).alias("rating_1_count"),
            count(
                when(col("sentiment") == "positive", 1)
            ).alias("sentiment_positive_count"),
            count(
                when(col("sentiment") == "neutral", 1)
            ).alias("sentiment_neutral_count"),
            count(
                when(col("sentiment") == "negative", 1)
            ).alias("sentiment_negative_count"),
        )
    )

    #display(fact_reviews)
    #display(df_rating_stats)

    #Final table
    dim_restaurants = spark.table("02_silver.dim_restaurants")

    d_restaurant_reviews = (
        dim_restaurants
        .join(
            df_rating_stats,
            dim_restaurants.restaurant_id == df_rating_stats.restaurant_id,
            "left"
        )
        .select(
            "dim_restaurants.restaurant_id",
            col("name").alias('restaurant_name'),
            "city",
            "total_reviews",
            "avg_rating",
            "rating_5_count",
            "rating_4_count",
            "rating_3_count",
            "rating_2_count",
            "rating_1_count",
            "sentiment_positive_count",
            "sentiment_neutral_count",
            "sentiment_negative_count"
        )
    )

    #display(d_restaurant_reviews)
    return d_restaurant_reviews