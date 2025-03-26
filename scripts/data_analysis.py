import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

engine = create_engine(
    f"redshift+psycopg2://{os.getenv('REDSHIFT_USER')}:{os.getenv('REDSHIFT_PASSWORD')}@"
    f"{os.getenv('REDSHIFT_HOST')}:{os.getenv('REDSHIFT_PORT')}/{os.getenv('REDSHIFT_DB')}"
)


query = """
SELECT
    date,
    open,
    close,
    price_change
FROM
    dim_stock_data
ORDER BY
    date DESC
LIMIT 10
"""
df = pd.read_sql(query, engine)

print(df)