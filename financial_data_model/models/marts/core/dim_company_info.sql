{{
  config(
    materialized='table',
    dist='all',
    sort='symbol'
  )
}}

SELECT
  ci.symbol,
  ci.name,
  ci.sector,
  ci.industry,
  ci.country,
  ci.market_cap,
  ci.pe_ratio,
  ci.dividend_yield,
  ci.processing_date,
FROM {{ ref('stg_company_info') }} ci
LEFT JOIN (
  SELECT symbol, close 
  FROM {{ ref('stg_stock_prices') }}
  WHERE processing_date = (SELECT MAX(processing_date) FROM {{ ref('stg_stock_prices') }})
) sp ON ci.symbol = sp.symbol