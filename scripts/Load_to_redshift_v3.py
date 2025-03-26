#!/usr/bin/env python3
import os
import logging
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import boto3
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_config():
    """Load environment variables and configuration"""
    load_dotenv()
    return {
        's3_bucket': os.getenv("S3_BUCKET_NAME"),
        'prices_path': os.getenv("PRICES_S3_PATH", "processed_stock_data/daily_prices.csv"),
        'companies_path': os.getenv("COMPANIES_S3_PATH", "stock_data/companies_info.csv"),
        
        # Redshift configuration
        'redshift_host': os.getenv("REDSHIFT_HOST"),
        'redshift_port': os.getenv("REDSHIFT_PORT", "5439"),
        'redshift_db': os.getenv("REDSHIFT_DB"),
        'redshift_user': os.getenv("REDSHIFT_USER"),
        'redshift_password': os.getenv("REDSHIFT_PASSWORD"),
        'redshift_iam_role': os.getenv("REDSHIFT_IAM_ROLE"),
        
        # AWS configuration
        'aws_region': os.getenv("AWS_REGION", "eu-north-1")
    }

def create_redshift_connection(config):
    """Create connection to Redshift"""
    try:
        conn = psycopg2.connect(
            host=config['redshift_host'],
            port=config['redshift_port'],
            database=config['redshift_db'],
            user=config['redshift_user'],
            password=config['redshift_password']
        )
        logger.info("Successfully connected to Redshift")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to Redshift: {str(e)}")
        raise

def setup_redshift_tables(conn):
    """Create tables if they don't exist"""
    create_tables = [
        """
        CREATE TABLE IF NOT EXISTS stock_daily_price (
            symbol VARCHAR(10) NOT NULL,
            date DATE NOT NULL,
            open DECIMAL(12,4),
            high DECIMAL(12,4),
            low DECIMAL(12,4),
            close DECIMAL(12,4),
            volume BIGINT,
            price_change DECIMAL(12,4),
            daily_return DECIMAL(12,4),
            processing_date TIMESTAMP,
            PRIMARY KEY (symbol, date)
        )
        DISTKEY(symbol)
        SORTKEY(date);
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_company_info (
            symbol VARCHAR(10) PRIMARY KEY,
            name VARCHAR(100),
            sector VARCHAR(50),
            industry VARCHAR(50),
            country VARCHAR(50),
            market_cap DECIMAL(20,2),
            pe_ratio DECIMAL(10,2),
            dividend_yield DECIMAL(10,4),
            processing_date TIMESTAMP
        )
        DISTSTYLE ALL;
        """
    ]
    
    try:
        with conn.cursor() as cur:
            for table_ddl in create_tables:
                cur.execute(table_ddl)
            conn.commit()
        logger.info("Redshift tables created/verified")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating tables: {str(e)}")
        raise

def load_data_to_redshift(conn, config):
    """Load data from S3 to Redshift using COPY command"""
    processing_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    load_commands = [
        {
            'table': 'stock_daily_price',
            's3_path': f"s3://{config['s3_bucket']}/{config['prices_path']}",
            'columns': ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume', 
                       'price_change', 'daily_return', 'processing_date'],
            'options': "FORMAT CSV, HEADER, DATEFORMAT 'auto', TIMEFORMAT 'auto'"
        },
        {
            'table': 'dim_company_info',
            's3_path': f"s3://{config['s3_bucket']}/{config['companies_path']}",
            'columns': ['symbol', 'name', 'sector', 'industry', 'country', 
                       'market_cap', 'pe_ratio', 'dividend_yield', 
                       'processing_date'],
            'options': "FORMAT CSV, HEADER"
        }
    ]
    
    try:
        with conn.cursor() as cur:
            for cmd in load_commands:
                # Truncate target table first (or use MERGE for incremental loads)
                cur.execute(f"TRUNCATE TABLE {cmd['table']}")
                
                # Build COPY command
                copy_sql = sql.SQL("""
                    COPY {table} ({columns})
                    FROM '{s3_path}'
                    IAM_ROLE '{iam_role}'
                    {options}
                """).format(
                    table=sql.Identifier(cmd['table']),
                    columns=sql.SQL(', ').join(map(sql.Identifier, cmd['columns'])),
                    s3_path=sql.Literal(cmd['s3_path']),
                    iam_role=sql.Literal(config['redshift_iam_role']),
                    options=sql.SQL(cmd['options'])
                )
                
                logger.info(f"Loading data into {cmd['table']} from {cmd['s3_path']}")
                cur.execute(copy_sql)
                conn.commit()
                logger.info(f"Successfully loaded data into {cmd['table']}")
                
                # Update processing date
                update_sql = f"""
                    UPDATE {cmd['table']}
                    SET processing_date = '{processing_date}'
                    WHERE processing_date IS NULL
                """
                cur.execute(update_sql)
                conn.commit()
                
    except Exception as e:
        conn.rollback()
        logger.error(f"Error loading data to Redshift: {str(e)}")
        raise

def main():
    try:
        # Load configuration
        config = load_config()
        
        # Create Redshift connection
        conn = create_redshift_connection(config)
        
        # Setup tables
        setup_redshift_tables(conn)
        
        # Load data from S3 to Redshift
        load_data_to_redshift(conn, config)
        
        logger.info("Data loading to Redshift completed successfully")
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Redshift connection closed")

if __name__ == "__main__":
    main()