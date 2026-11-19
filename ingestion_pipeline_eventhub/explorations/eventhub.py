# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

EH_NAMESPACE = 'eh-orders-dbx'
EH_NAME = 'orders'
EH_CONN_STR = 'get the stored connection string from the secret scope, create the secret scope first if needed'

# COMMAND ----------

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

# COMMAND ----------

# DBTITLE 1,Preview Event Hubs stream
import uuid

df_raw = (
    spark.readStream
    .format("kafka")
    .options(**{**KAFKA_OPTIONS, 'checkpointLocation': '/Volumes/ws_adbdbx/default/checkpoints/eventhub_explore'})
    .load()
)

display(df_raw, checkpointLocation=f'/Volumes/ws_adbdbx/default/checkpoints/eventhub_explore/{uuid.uuid4()}')

# COMMAND ----------

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

# COMMAND ----------

# DBTITLE 1,Parse key and value as strings
import uuid

df_parsed = (
    df_raw
    .withColumn("key_str", F.col("key").cast("string"))
    .withColumn("value_str", F.col("value").cast("string"))
    .withColumn("data", F.from_json(F.col("value_str"), _schmea))
    .select("data.*")
    .withColumnRenamed("timestamp", "order_timestamp")
)


display(df_parsed, checkpointLocation=f'/Volumes/ws_adbdbx/default/checkpoints/eventhub_explore_parsed/{uuid.uuid4()}')

# COMMAND ----------

