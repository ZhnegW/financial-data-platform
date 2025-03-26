{{
  config(
    materialized='incremental',
    unique_key='id',
    dist='symbol',
    sort='date'
  )
}}

WITH source AS (
  SELECT 
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume,
    price_change,
    daily_return,
    processing_date,
    {{ dbt_utils.generate_surrogate_key(['symbol', 'date']) }} as id
  FROM {{ source('stock_source', 'stock_prices') }}
  {% if is_incremental() %}
  WHERE processing_date > (SELECT MAX(processing_date) FROM {{ this }})
  {% endif %}
)