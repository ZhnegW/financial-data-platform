import os
import requests
import pandas as pd
import boto3
from datetime import datetime
from dotenv import load_dotenv
from io import StringIO

# Load environment variables
load_dotenv()

# Configuration
config = {
    'api_key': os.getenv("API_KEY"),
    'symbols': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'],  # add more stock symbols
    's3_bucket': os.getenv("S3_BUCKET_NAME"),
    'prices_key': 'stock_data/daily_prices.csv', 
    'companies_key': 'stock_data/companies_info.csv',
    'aws_access_key': os.getenv("AWS_ACCESS_KEY_ID"),
    'aws_secret_key': os.getenv("AWS_SECRET_ACCESS_KEY")
}

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=config['aws_access_key'],
    aws_secret_access_key=config['aws_secret_key']
)

def get_existing_data(s3_key):
    """Getting existing data from S3"""
    try:
        obj = s3.get_object(Bucket=config['s3_bucket'], Key=s3_key)
        return pd.read_csv(obj['Body'])
    except Exception as e:
        print(f"No existing data found for {s3_key} or error: {str(e)}")
        return None

def save_to_s3(df, s3_key):
    """Save DataFrame to S3"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(
        Bucket=config['s3_bucket'],
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )
    print(f"Successfully saved data to {s3_key}")

def fetch_stock_prices(symbol, existing_prices=None):
    """Get stock price data"""
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={config['api_key']}&outputsize=full"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if "Time Series (Daily)" not in data:
            print(f"No price data found for {symbol}")
            return None
            
        df = pd.DataFrame(data["Time Series (Daily)"]).T
        df.reset_index(inplace=True)
        df.columns = ["date", "open", "high", "low", "close", "volume"]
        df['symbol'] = symbol
        
        # Keep only new data
        if existing_prices is not None:
            existing_dates = existing_prices[existing_prices['symbol'] == symbol]['date']
            df = df[~df['date'].isin(existing_dates)]
        
        return df
    except Exception as e:
        print(f"Error fetching prices for {symbol}: {str(e)}")
        return None

def fetch_company_info(symbol, existing_companies=None):
    """Get company information"""
    url = f"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={config['api_key']}"
    
    try:
        if existing_companies is not None and symbol in existing_companies['Symbol'].values:
            print(f"Company info for {symbol} already exists")
            return None
            
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if not data:
            print(f"No company info found for {symbol}")
            return None
            
        info_df = pd.DataFrame([{
            'Symbol': data.get('Symbol', ''),
            'Name': data.get('Name', ''),
            'Sector': data.get('Sector', ''),
            'Industry': data.get('Industry', ''),
            'Country': data.get('Country', ''),
            'MarketCap': data.get('MarketCapitalization', ''),
            'PERatio': data.get('PERatio', ''),
            'DividendYield': data.get('DividendYield', ''),
            'LastUpdated': datetime.now().strftime('%Y-%m-%d')
        }])
        
        return info_df
    except Exception as e:
        print(f"Error fetching company info for {symbol}: {str(e)}")
        return None

def main():
    # Get Existing Data
    existing_prices = get_existing_data(config['prices_key'])
    existing_companies = get_existing_data(config['companies_key'])
    
    all_prices = []
    all_companies = []
    
    for symbol in config['symbols']:
        # Get Stock Prices
        prices_df = fetch_stock_prices(symbol, existing_prices)
        if prices_df is not None:
            all_prices.append(prices_df)
        
        # Get Company Information
        company_df = fetch_company_info(symbol, existing_companies)
        if company_df is not None:
            all_companies.append(company_df)
    
    # Integrate data
    if all_prices:
        new_prices = pd.concat(all_prices, ignore_index=True)
        if existing_prices is not None:
            updated_prices = pd.concat([existing_prices, new_prices], ignore_index=True)
        else:
            updated_prices = new_prices
        save_to_s3(updated_prices, config['prices_key'])
    
    if all_companies:
        new_companies = pd.concat(all_companies, ignore_index=True)
        if existing_companies is not None:
            updated_companies = existing_companies[~existing_companies['Symbol'].isin(new_companies['Symbol'])]
            updated_companies = pd.concat([updated_companies, new_companies], ignore_index=True)
        else:
            updated_companies = new_companies
        save_to_s3(updated_companies, config['companies_key'])
    
    print("Data update completed!")

if __name__ == "__main__":
    main()