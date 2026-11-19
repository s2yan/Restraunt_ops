from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import *


EH_NAMESPACE = spark.conf.get("eh_namespace")
EH_NAME = spark.conf.get("eh_name")
EH_CONN_STR = spark.conf.get("eh_conn_str")

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : "60000",
  "kafka.session.timeout.ms" : "50000",
  "maxOffsetsPerTrigger"     : "30000",
  "failOnDataLoss"           : "true",
  "startingOffsets"          : "earliest"
}


@dp.table(table_properties={"quality" : "bronze"})
def orders():
    df_raw = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    _schmea = StructType([
        StructField("order_id",  StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("restaurant_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_type", StringType(), True),
        StructField("items", StringType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_status", StringType(), True)
    ])

    df_parsed = (
        df_raw
        .withColumn("key_str", F.col("key").cast("string"))
        .withColumn("value_str", F.col("value").cast("string"))
        .withColumn("data", F.from_json(F.col("value_str"), _schmea))
        .select("data.*")
        .withColumnRenamed("timestamp", "order_timestamp")
    )
    
    return df_parsed