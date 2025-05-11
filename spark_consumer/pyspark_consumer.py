import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, explode, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from dotenv import load_dotenv

load_dotenv('/app/.env')
# spark = SparkSession.builder \
#     .config("spark.jars", "/app/spark-snowflake_2.11-2.4.12-spark_2.4.jar,/app/snowflake-jdbc-3.13.21.jar") \
#     .appName("KafkaToKafkaWithDebug") \
#     .getOrCreate()
# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaToKafkaWithDebug") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schemas
trade_schema = StructType([
    StructField("c", ArrayType(StringType()), True),
    StructField("p", DoubleType(), True),
    StructField("s", StringType(), True),
    StructField("t", LongType(), True),
    StructField("v", DoubleType(), True)
])


# Read from Kafka

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.25.0.12:9092") \
    .option("subscribe", "trade_data") \
    .option("startingOffsets", "latest") \
    .load()
# Extract JSON part
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw") \
    .withColumn("start_idx", expr("locate('{', raw)")) \
    .withColumn("json_str", expr("substring(raw, start_idx, 1000)"))

# Parse JSON into struct
parsed_df = json_df.withColumn("parsed", from_json(col("json_str"), trade_schema))

# ✅ Check this by showing only parsed and raw
debug_df = parsed_df.select("parsed", "raw")

# # Extract and explode data safely
final_df = parsed_df.selectExpr(
    "parsed.c as c", 
    "parsed.p as p", 
    "parsed.s as s", 
    "parsed.t as t", 
    "parsed.v as v"
)

final_df.printSchema()

# Output to console
final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .start()

# Output to Kafka
final_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.25.0.12:9092") \
    .option("topic", "processed_trade_data") \
    .option("checkpointLocation", "/tmp/checkpoints/kafka") \
    .start()

# snowflake config
sfOptions = {
    "sfURL": os.environ['SNOWFLAKE_URL'],
    "sfUser": os.environ['SNOWFLAKE_USER'],
    "sfPassword": os.environ['SNOWFLAKE_PASSWORD'],
    "sfDatabase": os.environ['SNOWFLAKE_DATABASE'],
    "sfSchema": os.environ['SNOWFLAKE_SCHEMA'],
    "sfWarehouse": os.environ['SNOWFLAKE_WAREHOUSE'],
    "sfRole": os.environ['SNOWFLAKE_ROLE'],
    "dbtable": "try_table"# os.environ['SNOWFLAKE_TABLE']
}
print( os.environ.get('SNOWFLAKE_URL'))
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
# # Write to Snowflake using foreachBatch
# def write_to_snowflake(batch_df, batch_id):
#     batch_df.write \
#         .format(SNOWFLAKE_SOURCE_NAME) \
#         .options(**sfOptions) \
#         .mode("append") \
#         .save()

# query = final_df.writeStream \
#     .foreachBatch(write_to_snowflake) \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/checkpoints/snowflake") \
#     .start()
# write to snowflake
from pyspark.sql import functions as F
##sanity check
# Sanity-check write to Snowflake
test_df = spark.createDataFrame([{"name": "Test", "age": 28}])
test_df.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**sfOptions) \
    .mode("overwrite") \
    .save()
# Define the function to write to Snowflake if the DataFrame is not empty
# def write_to_snowflake(batch_df, batch_id):
#     # Check if the batch DataFrame is empty
#     if batch_df.count() == 0:
#         print("Batchis empty. Skipping write to Snowflake.") # print(f"Batch {batch_id} "is empty. Skipping write to Snowflake.")
#         return  # Skip writing to Snowflake if the batch is empty
    
#     try:
#         # Proceed with writing to Snowflake if the DataFrame is not empty
#         print("Writing batch with str(batch_df.count())records") # print(f"Writing batch {batch_id} with {batch_df.count()} records")
#         batch_df.write \
#             .format("snowflake") \
#             .options(**sfOptions) \
#             .mode("append") \
#             .save()
#     except Exception as e:
#         print("Error in writing to Snowflake")
#         print(e) # print(f"Error in writing to Snowflake: {e}")

# # Your structured streaming logic
# final_df.writeStream \
#     .foreachBatch(write_to_snowflake) \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/checkpoints/snowflake") \
#     .start()

# Final wait
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print("Streaming query error:", {e})
