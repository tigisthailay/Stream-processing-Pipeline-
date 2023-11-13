from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

if __name__ == "__main__":
    
    #Create Spark Session
    spark = (
        SparkSession.builder.appName("Kafka Pyspark Stream Processing")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    #Connect Spark Streaming with Kafka topic to read Data Streams
    KAFKA_TOPIC_NAME = os.environ['KAFKA_TOPIC_NAME']
    KAFKA_SINK_TOPIC = os.environ['KAFKA_SINK_TOPIC']
    KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

    sample_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
            .option("subscribe", KAFKA_TOPIC_NAME)
            .option("startingOffsets", "latest")
            .load()
        )
    #Extract Topic information and apply suitable schema
    base_df = sample_df.selectExpr("CAST(value as STRING)", "timestamp")
    base_df.printSchema()

    #Applying suitable schema
    sample_schema = (
            StructType()
            .add("c", StringType())
            .add("p", StringType())
            .add("s", StringType())
            .add("t", StringType())
            .add("v", StringType())
        )

    info_df = base_df.select( from_json(col("value"), sample_schema).alias("info"), "timestamp" )
    #info_df.printSchema()

    #explode the multi-dimensional data to a 1D data frame by selecting suitable data
    info_df_fin = info_df.select("info.*", "timestamp")
    #info_df_fin.printSchema() 

    #Creating query using structured streaming
    query = info_df_fin.groupBy("s").agg(
        count(col("v")).alias("volume"),
    )

 #Expose the calculated results back to Kafka
    result_1 = query.selectExpr(
        "CAST(c AS STRING)",
        "CAST(col_p_alias AS STRING)",
        "CAST(symbole",
    ).withColumn("value", to_json(struct("*")).cast("string"),)

    result = (
        result_1.select("value")
        .writeStream.trigger(processingTime="10 seconds")
        .outputMode("complete")
        .format("kafka")
        .option("topic", KAFKA_SINK_TOPIC)
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
        .start()
        .awaitTermination()
    )
