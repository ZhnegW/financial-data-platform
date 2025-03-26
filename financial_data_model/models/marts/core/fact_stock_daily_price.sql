{{
  config(
    materialized='incremental',
    unique_key='id',
    dist='symbol',
    sort=['symbol', 'date'],
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "month"
    }
  )
}}

SELECT
  id,
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
FROM {{ ref('stg_stock_prices') }}
{% if is_incremental() %}
WHERE processing_date > (SELECT MAX(processing_date) FROM {{ this }})
{% endif %}