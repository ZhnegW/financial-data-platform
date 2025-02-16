# from pyspark.sql import SparkSession

# spark = SparkSession.builder.getOrCreate()
# df = spark.read.option("header", True).csv("s3a://financialdataplatform/raw_stock_data.csv")
# df.show(5)

# import os

# print("AWS_ACCESS_KEY_ID:", os.getenv("AWS_ACCESS_KEY_ID"))
# print("AWS_SECRET_ACCESS_KEY:", os.getenv("AWS_SECRET_ACCESS_KEY"))

import os

print(os.getenv("REDSHIFT_HOST"))
print(os.getenv("REDSHIFT_DATABASE"))
print(os.getenv("REDSHIFT_PORT"))
print(os.getenv("REDSHIFT_USER"))
print(os.getenv("REDSHIFT_PASSWORD"))