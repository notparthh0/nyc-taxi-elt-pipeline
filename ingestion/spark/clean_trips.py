from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, unix_timestamp

print("Starting Spark...")
spark = SparkSession.builder \
    .appName("NYC Taxi Cleaning") \
    .master("local[*]") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Reading January 2023 data...")
df = spark.read.parquet("data/raw/yellow_tripdata_2023-01.parquet")
print(f"Total raw rows: {df.count()}")

print("Cleaning data...")
df_clean = df.filter(
    (col("fare_amount") > 0) &
    (col("trip_distance") > 0) &
    (col("passenger_count") >= 1) &
    (col("total_amount") > 0) &
    (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime"))
)

df_clean = df_clean.withColumn(
    "trip_duration_minutes",
    round(
        (unix_timestamp("tpep_dropoff_datetime") -
         unix_timestamp("tpep_pickup_datetime")) / 60,
        2
    )
).withColumn(
    "cost_per_mile",
    round(col("total_amount") / col("trip_distance"), 2)
)

df_clean = df_clean.filter(col("trip_duration_minutes") <= 180)

print(f"Total cleaned rows: {df_clean.count()}")

print("Saving to data/cleaned/...")
df_clean.write.mode("overwrite").parquet("data/cleaned/")
print("Done!")

spark.stop()
