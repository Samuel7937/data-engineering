from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


# 1. Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStructuredStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# 2. Reading from Kafka
kafka_bootstrap_servers = "18.211.252.152:9092"
kafka_topic = "real-time-project"

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()


# 3. Defining schema for JSON
item_schema = StructType([
    StructField("SKU", StringType(), True),
    StructField("title", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", IntegerType(), True)
])

schema = StructType([
    StructField("items", ArrayType(item_schema), True),
    StructField("type", StringType(), True),
    StructField("country", StringType(), True),
    StructField("invoice_no", LongType(), True),
    StructField("timestamp", StringType(), True)
])


# 4. Parse JSON from Kafka
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Convert timestamp and filter nulls
parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")
).filter(col("event_time").isNotNull())

# 5. Explode items array
exploded_df = parsed_df.withColumn("item", explode("items"))

# Extract fields from struct
flat_df = exploded_df.select(
    "invoice_no", "timestamp", "country", "type",
    col("item.SKU").alias("SKU"),
    col("item.title").alias("title"),
    col("item.unit_price").alias("unit_price"),
    col("item.quantity").alias("quantity"),
    "event_time"
)


# 6. Calculations

# Make returns negative
flat_df = flat_df.withColumn(
    "total_cost",
    when(col("type") == "RETURN", -1 * col("unit_price") * col("quantity"))
    .otherwise(col("unit_price") * col("quantity"))
)

flat_df = flat_df.withColumn("total_items", col("quantity"))

# Flags
flat_df = flat_df.withColumn("is_order", when(col("type") == "ORDER", 1).otherwise(0))
flat_df = flat_df.withColumn("is_return", when(col("type") == "RETURN", 1).otherwise(0))


# 7. Aggregations for KPIs
# Base aggregated metrics (global)
base_agg = flat_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute")) \
    .agg(
        sum("total_cost").alias("total_sales_volume"),
        approx_count_distinct("invoice_no").alias("opm"),
        sum("is_order").alias("total_orders"),
        sum("is_return").alias("total_returns")
    )

# Compute global KPIs (4 final columns)
aggregated_df = base_agg \
    .withColumn(
        "rate_of_return",
        when((col("total_orders") + col("total_returns")) > 0,
             col("total_returns") / (col("total_orders") + col("total_returns")))
        .otherwise(lit(0))
    ).withColumn(
        "average_transaction_size",
        when((col("total_orders") + col("total_returns")) > 0,
             col("total_sales_volume") / (col("total_orders") + col("total_returns")))
        .otherwise(lit(0))
    ) \
    .select("window", "total_sales_volume", "opm", "rate_of_return", "average_transaction_size")

# Country-wise KPIs (3 final columns)
country_base_agg = flat_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "1 minute"), col("country")) \
    .agg(
        sum("total_cost").alias("total_sales_volume"),
        approx_count_distinct("invoice_no").alias("opm"),
        sum("is_order").alias("total_orders"),
        sum("is_return").alias("total_returns")
    )

country_kpi_df = country_base_agg \
    .withColumn(
        "rate_of_return",
        when((col("total_orders") + col("total_returns")) > 0,
             col("total_returns") / (col("total_orders") + col("total_returns")))
        .otherwise(lit(0))
    ) \
    .select("window", "country", "total_sales_volume", "opm", "rate_of_return")


# 8. OUTPUT STREAMS

# (1) Detailed table in console
flat_query = flat_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 50) \
    .trigger(processingTime="1 minute") \
    .start()

# (2) Global KPI aggregations in console
query = aggregated_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 50) \
    .trigger(processingTime="1 minute") \
    .start()

# (3) Time-based KPIs to JSON
time_kpi_json = aggregated_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/home/hadoop/output/Timebased-KPI") \
    .option("checkpointLocation", "/home/hadoop/checkpoints/Timebased-KPI") \
    .trigger(processingTime="1 minute") \
    .start()

# (4) Country+time-based KPIs to JSON
country_kpi_json = country_kpi_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "/home/hadoop/output/Country-and-timebased-KPI") \
    .option("checkpointLocation", "/home/hadoop/checkpoints/Country-and-timebased-KPI") \
    .trigger(processingTime="1 minute") \
    .start()

spark.streams.awaitAnyTermination()

