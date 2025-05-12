import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, explode, expr, avg, sum as _sum, max as _max, collect_list
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from dotenv import load_dotenv
 
load_dotenv('/app/.env')

kafka_topic_name = os.getenv("KAFKA_TOPIC_NAME")
kafka_sink_topic = os.getenv("KAFKA_SINK_TOPIC")
kafka_bootstrap_server = f"{os.getenv('KAFKA_SERVER')}:{os.getenv('KAFKA_PORT')}"

spark = SparkSession.builder \
    .appName("StreamProcessing") \
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
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topic_name) \
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
def transform_and_aggregate(df):
    exploded_df = df.withColumn("client", explode("c"))
    aggregated_df = exploded_df.groupBy("s").agg(
        avg("p").alias("avg_price"),
        _sum("v").alias("total_volume"),
        _max("t").alias("latest_timestamp"),
        collect_list("client").alias("clients")
    )
    return aggregated_df

# Output to console
final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 20) \
    .start()

aggregated_df = transform_and_aggregate(final_df)
# aggregated_df.printSchema()
aggregated_df.show()

# Output to Kafka
aggregated_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("topic", kafka_sink_topic) \
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
# print( os.environ.get('SNOWFLAKE_URL'))
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# Define the function to write to Snowflake if the DataFrame is not empty
def write_to_snowflake(batch_df, batch_id):
    if batch_df.count() == 0:
        print("Batch is empty. Skipping write to Snowflake.")
        return
    try:
        print("Writing batch", {batch_id}, "with", {batch_df.count()} ,"records to Snowflake.")
        batch_df.write \
            .format(SNOWFLAKE_SOURCE_NAME) \
            .options(**sfOptions) \
            .mode("append") \
            .save()
    except Exception as e:
        print("Error in writing to Snowflake:", e)

# # writing each batch to snowflake
# final_df.writeStream \
#     .foreachBatch(write_to_snowflake) \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/checkpoints/snowflake") \
#     .start()


# write the aggregated data to snowflake
# aggregated_df.writeStream \
#     .foreachBatch(write_to_snowflake) \
#     .outputMode("update") \
#     .option("checkpointLocation", "/tmp/checkpoints/snowflake_aggregated") \
#     .start()



# Final wait
try:
    spark.streams.awaitAnyTermination()
except Exception as e:
    print("Streaming query error:", {e})
