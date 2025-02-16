import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# engine = create_engine(
#     f"redshift+psycopg2://{os.getenv('REDSHIFT_USER')}:{os.getenv('REDSHIFT_PASSWORD')}@"
#     f"{os.getenv('REDSHIFT_HOST')}:{os.getenv('REDSHIFT_PORT')}/{os.getenv('REDSHIFT_DB')}"
# )

# upload transformed data from local to redshift

DATABASE_URL = "redshift+psycopg2://awsuser:Wz12345678@redshift-cluster-1.cwo1lpfqwelu.eu-north-1.redshift.amazonaws.com:5439/dev"
engine = create_engine(DATABASE_URL)
df = pd.read_parquet("data/cleaned_stock_data.parquet")

# write to Redshift
with engine.connect() as connection:
    df.to_sql("stock_data", connection, schema="public", if_exists="replace", index=False)

print("The data has been successfully loaded into Redshift!")