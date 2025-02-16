import os
import requests
import pandas as pd
import boto3
from dotenv import load_dotenv

# fetch raw data from Alpha Vantage API -> store as CSV locally -> upload to s3

# load env variables
load_dotenv()

# Alpha Vantage API
API_KEY = os.getenv("API_KEY")
SYMBOL = "AAPL"  # apple stock
URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&apikey={API_KEY}"

# obtain data
response = requests.get(URL)
data = response.json()

# convert to DataFrame
df = pd.DataFrame(data["Time Series (Daily)"]).T
df.reset_index(inplace=True)
df.columns = ["date", "open", "high", "low", "close", "volume"]

# store as CSV
os.makedirs("data", exist_ok=True)  # make sure data folder exists
df.to_csv("data/raw_stock_data.csv", index=False)

# upload to AWS S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)
s3.upload_file("data/raw_stock_data.csv", os.getenv("S3_BUCKET_NAME"), "raw_stock_data.csv")

print("Data has been successfully acquired and uploaded to S3！")