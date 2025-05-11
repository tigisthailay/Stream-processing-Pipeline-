FROM gettyimages/spark:2.4.1-hadoop-3.0

# Set working directory
WORKDIR /app

# Copy your consumer script and JAR connectors
COPY pyspark_consumer.py /app/
COPY requirements.txt /app/ 
COPY jars/spark-sql-kafka-0-10_2.11-2.4.5.jar /app/
COPY jars/kafka-clients-2.2.0.jar /app/
COPY jars/snowflake-jdbc-3.12.17.jar /app/
COPY jars/spark-snowflake_2.11-2.4.14-spark_2.4.jar /app/
# COPY snowflake-jdbc-3.13.21.jar /app/
# COPY spark-snowflake_2.11-2.4.12-spark_2.4.jar /app/

# Install dependencies
RUN pip install --upgrade pip && pip install --default-timeout=100 --no-cache-dir -r requirements.txt

# Execute the Spark job on container start
ENTRYPOINT ["spark-submit", "--jars", "/app/spark-sql-kafka-0-10_2.11-2.4.5.jar,/app/kafka-clients-2.2.0.jar", "--driver-class-path", "/app/kafka-clients-2.2.0.jar", "/app/pyspark_consumer.py"]
# ENTRYPOINT ["spark-submit", "--jars", "/app/spark-sql-kafka-0-10_2.11-2.4.5.jar,/app/kafka-clients-2.2.0.jar,/app/spark-snowflake_2.11-2.4.12-spark_2.4.jar,/app/snowflake-jdbc-3.13.21.jar", "--driver-class-path", "/app/kafka-clients-2.2.0.jar:/app/snowflake-jdbc-3.13.21.jar", "/app/pyspark_consumer.py"]
