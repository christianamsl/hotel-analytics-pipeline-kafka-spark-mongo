from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import current_timestamp, unix_timestamp, col, lit

# Εκκίνηση SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkAggregator") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
simulation_start = datetime.now()

simulation_start_literal = lit(simulation_start.isoformat())


# Ανάγνωση από Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "website_actions") \
    .option("startingOffsets", "latest") \
    .load()



schema = StructType([
    StructField("user_id", StringType()),
    StructField("session_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("event_type", StringType()),
    StructField("booking_id", StringType()),
    StructField("check_in_date", StringType()),
    StructField("check_out_date", StringType()),
    StructField("hotel_id", StringType()),
    StructField("item_id", StringType()),
    StructField("location", StringType()),  # <-- destination_name 
    StructField("num_guests", StringType()),
    StructField("page_url", StringType()),
    StructField("payment_method", StringType()),
    StructField("price", StringType()),
    StructField("room_type", StringType()),
    StructField("total_price", StringType())
])


# Ανάλυση JSON
df_json = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# μετατροπή string -> double
df_json = df_json.withColumn("price", when(col("price") != "", col("price").cast("double")))
df_json = df_json.withColumn("total_price", when(col("total_price") != "", col("total_price").cast("double")))
# Μετατροπή χρόνου
df_json = df_json.withColumn("event_time", to_timestamp("timestamp"))

#-----------------------ερωτημα 3 : Mongo raw data----------------------------------------------------------------
def write_raw_to_mongo(batch_df, batch_id):
    print(f"[INFO] Writing batch {batch_id} with {batch_df.count()} rows to MongoDB...")
    batch_df.write \
        .format("mongodb") \
        .option("uri", "mongodb://localhost:27017") \
        .option("database", "clickstream") \
        .option("collection", "raw_events") \
        .mode("append") \
        .save()

raw_query = df_json.writeStream \
    .foreachBatch(write_raw_to_mongo) \
    .outputMode("append") \
    .start()

#---------------------------------------------------------------------------------------

allowed_event_types = ["search_hotels", "complete_booking"]
df_clean = df_json \
    .withColumn("event_type", trim(lower(col("event_type")))) \
    .withColumn("location", trim(col("location"))) \
    .filter((col("event_type").isin(allowed_event_types)) & (col("location") != ""))

# Aggregation 
result = df_clean \
    .withWatermark("event_time", "1 second") \
    .groupBy( 
        "location"
    ).agg(
        count(when(col("event_type") == "search_hotels", True)).alias("search_volume"),
        count(when(col("event_type") == "complete_booking", True)).alias("bookings_volume"),
        coalesce(sum(when(col("event_type") == "complete_booking", col("total_price"))), lit(0.0)).alias("sales_volume")
    )



# Χρονική διαφορά σε λεπτά από την έναρξη της εξομοίωσης
df_with_time_diff = result.withColumn(
    "time", 
    (unix_timestamp(current_timestamp()) - lit(int(simulation_start.timestamp()))) / 60
).withColumn(
    "time", col("time").cast("int")
)


# Επιλογή πεδίων
final = df_with_time_diff.select(
    "time",
    col("location").alias("destination_name"),
    "search_volume",
    "bookings_volume",
    "sales_volume"
)
#----------------ερωτημα 3 : Mongo aggregated data-----------------------------------------------------------------------
def write_agg_to_mongo(batch_df, batch_id):
    print(f"[INFO] Writing batch {batch_id} with {batch_df.count()} rows to MongoDB...")
    batch_df.write \
        .format("mongodb") \
        .option("uri", "mongodb://localhost:27017") \
        .option("database", "clickstream") \
        .option("collection", "aggregated_stats") \
        .mode("append") \
        .save()

agg_query = final.writeStream \
    .foreachBatch(write_agg_to_mongo) \
    .outputMode("update") \
    .trigger(processingTime="5 minutes") \
    .start()
#------------------------------------------------------------------------------------------
# Εκτύπωση στην κονσόλα
query = final.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 minutes") \
    .start()

raw_query.awaitTermination()
agg_query.awaitTermination()
query.awaitTermination()
