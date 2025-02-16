import os
from dotenv import load_dotenv
import redshift_connector

# load cleaned and transformed data parquet file from S3 bucket to Redshift

load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_FILE_KEY = "cleaned_stock_data.parquet"

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")

conn = redshift_connector.connect(
     host= REDSHIFT_HOST,
     database= REDSHIFT_DB,
     port= REDSHIFT_PORT,
     user= REDSHIFT_USER,
     password= REDSHIFT_PASSWORD,
  )
cursor = conn.cursor()

create_table_sql = """
CREATE TABLE IF NOT EXISTS public.cleaned_stock_data (
    date FLOAT,
    "open" FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume INT,
    price_change FLOAT
);
"""
cursor.execute(create_table_sql)
conn.commit()
print("Table 'cleaned_stock_data' created successfully.")


copy_sql = f"""
COPY public.cleaned_stock_data
FROM 's3://{S3_BUCKET_NAME}/{S3_FILE_KEY}'
CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY};aws_secret_access_key={AWS_SECRET_KEY}'
FORMAT AS PARQUET;
"""
cursor.execute(copy_sql)
conn.commit()

print("Parquet file loaded successfully from S3.")

cursor.close()
conn.close()
