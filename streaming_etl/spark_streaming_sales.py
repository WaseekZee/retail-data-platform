from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("RetailSalesStream")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")  # let this pull kafka-clients 3.9.0
    .getOrCreate()
)

schema = StructType([
    StructField("event_time", StringType()),
    StructField("store_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("currency", StringType()),
    StructField("transaction_id", StringType()),
    StructField("channel", StringType()),
    StructField("total_amount", DoubleType())
])

sales_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "sales")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
)

sales_df = (
    sales_stream.selectExpr("CAST(value AS STRING) AS value")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        # If your time is ISO like 2025-08-22T10:15:30Z, use a pattern:
        # .withColumn("event_ts", to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ssXXX"))
        .withColumn("event_ts", to_timestamp("event_time"))
)

filtered_sales = sales_df.filter(col("total_amount") > 20)

query = (
    filtered_sales.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", "./checkpoints/sales_console")
        .trigger(processingTime="5 seconds")          # optional but nice for dev
        .start()
)

query.awaitTermination()
