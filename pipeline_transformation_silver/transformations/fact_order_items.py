from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp


@dp.table(name="fact_order_items", table_properties={"quality": "silver"})
@dp.expect_all_or_fail(
    {
        "valid order_id" : "order_id is not NULL",
        "valid item_id" : "item_id is not NULL",
        "valid restaurant_id" : "restaurant_id is not NULL",
        "valid order_timestamp" : "order_timestamp is not NULL",
        "valid quantity" : "quantity > 0",
        "valid unit_price" : "unit_price > 0",
        "valid sub_total" : "sub_total > 0"
    }
)
def fact_order_items():
    _schema = ArrayType(
        StructType([
        StructField("item_id", StringType()),
        StructField("name", StringType()),
        StructField("category", StringType()),
        StructField("quantity", IntegerType()),
        StructField("unit_price", DecimalType(10,2)),
        StructField("subtotal", DecimalType(10,2))
    ])
    ) 

    df_fact_order_items = (
        dp.read_stream("01_bronze.orders")
        .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
        .withColumn("order_date", to_date(col('order_timestamp')))
        .withColumn("item_parsed", from_json(col("items"), _schema))
        .withColumn("items", explode(col("item_parsed")))
        .select(
            "order_id",
            col("items.item_id").alias("item_id"),
            "restaurant_id",
            "order_timestamp",
            "order_date",
            col("items.name").alias("item_name"),
            col("items.quantity").alias("quantity"),
            col("items.unit_price").cast("decimal(10,2)").alias("unit_price"),
            col("items.subtotal").cast("decimal(10,2)").alias("sub_total")
        )
    )


    return df_fact_order_items
