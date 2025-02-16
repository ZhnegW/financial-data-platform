import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import boto3
from dotenv import load_dotenv
from s3_uploader import upload_folder_to_s3

# (download raw_data.csv from s3) -> local -> clean and transform data with spark -> store locally as parquet file -> upload to s3

# load env variables
load_dotenv()

# initialize Spark
spark = SparkSession.builder \
    .appName("FinancialDataCleaning") \
    .getOrCreate()

# Download raw data from S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
s3.download_file(
    os.getenv("S3_BUCKET_NAME"),
    "raw_stock_data.csv",
    "data/raw_stock_data.csv"
)

# Read raw data
df = spark.read.csv("data/raw_stock_data.csv", header=True, inferSchema=True)

df.printSchema()

# Clean data
df_cleaned = df.dropna()  # Delete missing values
df_cleaned = df_cleaned.withColumn("date", col("date").cast("date"))  # Converting date formats
df_cleaned.show(5)

# Data conversion: calculating daily price fluctuations
from pyspark.sql.functions import expr

df_transformed = df_cleaned.withColumn(
    "price_change",
    expr("close - open")
)

df_transformed.show(5)

# Store as Parquet file
df_transformed.write.mode("overwrite").parquet("data\cleaned_stock_data.parquet")

# Upload to S3
local_folder = r"data\cleaned_stock_data.parquet"
bucket_name = os.getenv("S3_BUCKET_NAME")
s3_folder = "cleaned_stock_data.parquet"
upload_folder_to_s3(local_folder, bucket_name, s3_folder, s3)
# s3.upload_file(
#     "data/cleaned_stock_data.parquet",
#     os.getenv("S3_BUCKET_NAME"),
#     "cleaned_stock_data.parquet"
# )

print("Data cleansing and transformation completed and uploaded to S3！")