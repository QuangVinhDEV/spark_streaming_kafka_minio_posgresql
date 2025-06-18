from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, length, trim
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, LongType
import os

# Load environment variables
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "raw_data")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "streaming-data")
POSTGRES_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'streaming_db')}"
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

print("=== CONFIGURATION ===")
print(f"KAFKA_BROKER       = {KAFKA_BROKER}")
print(f"KAFKA_RAW_TOPIC    = {RAW_TOPIC}")
print(f"MINIO_ENDPOINT     = {MINIO_ENDPOINT}")
print(f"MINIO_BUCKET       = {MINIO_BUCKET}")
print(f"POSTGRES_URL       = {POSTGRES_URL}")
print("=====================\n")

# Spark Session initialization
print("Initializing SparkSession...")
spark = SparkSession.builder \
    .appName("Spark Streaming Job") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.11.1026"
    ])) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("SparkSession initialized\n")

# Define schema
schema = StructType() \
    .add("user_id", StringType()) \
    .add("timestamp", LongType()) \
    .add("sentiment", StringType()) \
    .add("message", StringType())
print("📜 Schema defined\n")

# Read from Kafka
print(f"Reading stream from Kafka topic '{RAW_TOPIC}'...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", RAW_TOPIC) \
    .load()
print("Kafka stream ready\n")

# Parse the JSON messages
print("parsing JSON from Kafka messages...")
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")
print("JSON parsed\n")

# Convert timestamp to readable format
json_df = json_df.withColumn("timestamp", from_unixtime(col("timestamp")).cast(TimestampType()))

# select validation layers
validated_df = json_df \
    .filter(
        col("user_id").isNotNull() &
        col("timestamp").isNotNull() &
        col("sentiment").isNotNull() &
        col("message").isNotNull()
    ) \
    .filter(
        col("sentiment").isin("positive", "negative", "neutral")
    ) \
    .filter(
        (length(trim(col("message"))) > 3) &                     
        (col("message").rlike("^[a-zA-Z0-9\\s\\.,!?'-]+$"))      
    ) \
    .filter(
        col("user_id").rlike("^[a-zA-Z0-9_]+$")                
    )

validated_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
# Write to MinIO
print("Writing stream to MinIO (Parquet)...")
print(f"Writing to s3a://{MINIO_BUCKET}/processed/")
minio_sink = json_df.writeStream \
    .format("parquet") \
    .option("path", f"s3a://{MINIO_BUCKET}/processed/") \
    .option("checkpointLocation", f"s3a://{MINIO_BUCKET}/checkpoints/") \
    .option("failOnDataLoss", "false") \
    .outputMode("append") \
    .trigger(processingTime='10 seconds') \
    .start()
print("MinIO Sink isActive:", minio_sink.isActive)
print("Stream to MinIO started\n")

# Write to PostgreSQL
print("Writing stream to PostgreSQL table 'stream_metrics'...")
postgres_sink = validated_df.writeStream \
    .foreachBatch(lambda df, epochId: df.write \
        .format('jdbc') \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "stream_metrics") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .mode("append") \
        .save()) \
    .outputMode("update") \
    .start()
print("Stream to PostgreSQL started\n")

# Await termination
print("Streaming jobs running. Awaiting termination...\n")
spark.streams.awaitAnyTermination()
