#!/usr/bin/env python3

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import boto3
from dotenv import load_dotenv
from s3_uploader import upload_folder_to_s3

# read raw_data.csv directly from s3 -> clean and transform data with spark -> directly upload to s3 as parquet file

# load env variables
load_dotenv()
os.getenv("AWS_ACCESS_KEY_ID")
os.getenv("AWS_SECRET_ACCESS_KEY")

# initialize Spark
# spark = SparkSession.builder \
#     .appName("FinancialDataCleaning") \
#     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
#     .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
#     .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
#     .config("com.amazonaws.services.s3.enableV4", "true") \
#     .config("spark.hadoop.fs.s3a.endpoint", "eu-north-1.amazonaws.com") \
#     .getOrCreate()

# print("connect successfully!")
spark = SparkSession.builder.getOrCreate()

# Download raw data from S3
s3_path = f"s3a://{os.getenv('S3_BUCKET_NAME')}/raw_stock_data.csv"
df = spark.read.csv(s3_path, header=True, inferSchema=True)

df.printSchema()

# Clean data
df_cleaned = df.dropna()  # Delete missing values
df_cleaned = df_cleaned.withColumn("date", col("date").cast("date"))  # Converting date formats
df_cleaned.show(5)

# Data conversion: calculating daily price fluctuations
df_transformed = df_cleaned.withColumn(
    "price_change",
    expr("close - open")
)

df_transformed.show(5)

# Store as Parquet file
output_s3_path = f"s3a://{os.getenv('S3_BUCKET_NAME')}/cleaned_stock_data.parquet"
df_transformed.write.parquet(output_s3_path, mode="overwrite")

print("Data cleansing and transformation completed and uploaded to S3！")

spark.stop()