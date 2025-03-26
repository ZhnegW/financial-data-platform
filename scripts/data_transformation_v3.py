#!/usr/bin/env python3

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit
from pyspark.sql.types import DateType
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load configuration
def load_config():
    """Load environment variables and configuration"""
    load_dotenv()
    config = {
        'aws_access_key': os.getenv("AWS_ACCESS_KEY_ID"),
        'aws_secret_key': os.getenv("AWS_SECRET_ACCESS_KEY"),
        's3_bucket': os.getenv("S3_BUCKET_NAME"),
        's3_region': os.getenv("AWS_REGION", "eu-north-1"),
        'input_path': "stock_data/daily_prices.csv",
        'output_path': "processed_stock_data/daily_prices.csv",
        'processing_date': datetime.now().strftime('%Y-%m-%d')
    }
    return config

def create_spark_session(config):
    """Create and configure Spark session"""
    try:
        spark = SparkSession.builder \
            .appName("StockDataProcessing") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", config['aws_access_key']) \
            .config("spark.hadoop.fs.s3a.secret.key", config['aws_secret_key']) \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{config['s3_region']}.amazonaws.com") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise

def read_input_data(spark, config):
    """Read input data from S3"""
    try:
        input_path = f"s3a://{config['s3_bucket']}/{config['input_path']}"
        
        logger.info(f"Reading data from: {input_path}")
        df = spark.read.csv(
            input_path,
            header=True,
            inferSchema=True,
            dateFormat="yyyy-MM-dd"
        )
        
        # Data validation
        required_columns = {'date', 'open', 'high', 'low', 'close', 'volume'}
        if not required_columns.issubset(set(df.columns)):
            missing = required_columns - set(df.columns)
            raise ValueError(f"Missing required columns: {missing}")
        
        logger.info(f"Successfully read data with {df.count()} rows")
        return df
    except Exception as e:
        logger.error(f"Error reading input data: {str(e)}")
        raise

def transform_data(df, config):
    """Clean and transform the data"""
    try:
        # Clean data
        logger.info("Cleaning data...")
        df_clean = df.dropna()
        
        # Cast date column
        df_clean = df_clean.withColumn("date", col("date").cast(DateType()))
        
        # Add processing metadata
        df_clean = df_clean.withColumn("processing_date", lit(config['processing_date']))
        
        # Calculate derived metrics
        logger.info("Calculating derived metrics...")
        df_transformed = df_clean.withColumn(
            "price_change",
            expr("round(close - open, 4)")
        ).withColumn(
            "daily_return",
            expr("round((close - open) / open * 100, 4)")
        ).withColumn(
            "volatility",
            expr("round((high - low) / low * 100, 4)")
        )
        
        logger.info("Data transformation completed")
        return df_transformed
    except Exception as e:
        logger.error(f"Error transforming data: {str(e)}")
        raise

def write_output_data(df, config):
    """Write processed data to S3"""
    try:
        output_path = f"s3a://{config['s3_bucket']}/{config['output_path']}"
        
        logger.info(f"Writing data to: {output_path}")
        df.write.mode("overwrite") \
            .csv(output_path)
        
        logger.info("Data successfully written to S3")
    except Exception as e:
        logger.error(f"Error writing output data: {str(e)}")
        raise

def main():
    try:
        # Load configuration
        config = load_config()
        
        # Initialize Spark
        spark = create_spark_session(config)
        
        # Read data
        df = read_input_data(spark, config)
        
        # Transform data
        df_transformed = transform_data(df, config)
        
        # Write output
        write_output_data(df_transformed, config)
        
        logger.info("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()