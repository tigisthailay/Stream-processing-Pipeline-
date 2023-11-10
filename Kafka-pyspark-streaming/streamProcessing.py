from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    
    #Create Spark Session
    spark = (
        SparkSession.builder.appName("Kafka Pyspark Streaming Pipeline")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    #Connect Spark Streaming with Kafka topic to read Data Streams
    KAFKA_TOPIC_NAME = "FinnhubTrade"
    KAFKA_SINK_TOPIC = "FinnhubTradeback"
    KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

    sampleDataframe = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
            .option("subscribe", KAFKA_TOPIC_NAME)
            .option("startingOffsets", "latest")
            .load()
        )
    #Extract Topic information and apply suitable schema
    base_df = sampleDataframe.selectExpr("CAST(value as STRING)", "timestamp")
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

    info_dataframe = base_df.select( from_json(col("value"), sample_schema).alias("info"), "timestamp" )

    info_dataframe.printSchema()
    info_df_fin = info_dataframe.select("info.*", "timestamp")
    info_df_fin.printSchema()

    #Creating query using structured streaming

    query = info_df_fin.groupBy("s").agg(
        count(col("v")).alias("volume"),
    )

    # query = query.withColumn("query", lit("QUERY3"))
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
