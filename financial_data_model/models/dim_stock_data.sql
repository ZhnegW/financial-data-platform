{{ config(materialized='table') }}

SELECT
    date,
    "open",
    high,
    low,
    close,
    volume,
    price_change
FROM
    {{ ref('stg_stock_data') }}