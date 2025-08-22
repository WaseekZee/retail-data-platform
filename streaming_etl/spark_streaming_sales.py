from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from pyspark.sql.types import *

# ---------- Spark session ----------
spark = (
    SparkSession.builder
    .appName("RetailSalesStream")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,org.postgresql:postgresql:42.7.3"
    )
    # optional, to force winutils dir:
    # .config("spark.driver.extraJavaOptions","-Dhadoop.home.dir=C:\\Winutils")
    # .config("spark.executor.extraJavaOptions","-Dhadoop.home.dir=C:\\Winutils")
    .getOrCreate()
)

# Optional: quiet the logs
spark.sparkContext.setLogLevel("WARN")

# ---------- Schema (must match your producer JSON) ----------
schema = StructType([
    StructField("event_time", StringType()),      # ISO string from producer
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

# ---------- Read Kafka ----------
raw = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "sales")
         .option("startingOffsets", "latest")
         .load()
)

# ---------- Parse JSON ----------
parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
       .select(from_json(col("json_str"), schema).alias("data"))
       .select("data.*")
)

# cast event_time to timestamp and add event_date
typed = (
    parsed
    .withColumn("event_ts", to_timestamp(col("event_time")))   # timestamptz on write
    .withColumn("event_date", to_date(col("event_ts")))
    .drop("event_time")
    .withColumnRenamed("event_ts", "event_time")
)

# ---------- Optional: basic sanity filter ----------
clean = typed.filter(
    (col("quantity") > 0) &
    (col("unit_price") > 0) &
    (col("total_amount") >= col("unit_price") * col("quantity") * 0.8) &  # loose guard
    (col("total_amount") <= col("unit_price") * col("quantity") * 1.2)
)

# ---------- Deduplicate within each microbatch ----------
deduped = clean.dropDuplicates(["transaction_id"])

# ---------- JDBC write function ----------
jdbc_url = "jdbc:postgresql://localhost:5432/retail_dw"
jdbc_props = {
    "user": "retail",
    "password": "retailpwd",
    "driver": "org.postgresql.Driver"
}
target_table = "public.sales_stream"

def write_to_postgres(batch_df, batch_id: int):
    # Select columns in the exact order of the target table
    cols = [
        "transaction_id",
        "event_time",
        "event_date",
        "store_id",
        "product_id",
        "product_name",
        "unit_price",
        "quantity",
        "total_amount",
        "channel",
        "currency"
    ]
    (
        batch_df.select(*cols)
                .write
                .mode("append")
                .jdbc(url=jdbc_url, table=target_table, properties=jdbc_props)
    )

# ---------- Start stream with checkpointing ----------
query = (
    deduped.writeStream
          .outputMode("update")  # required for dropDuplicates; data itself written via foreachBatch
          .foreachBatch(write_to_postgres)
          .option("checkpointLocation", "streaming_etl/_checkpoints/sales_to_pg")
          .start()
)

query.awaitTermination()
