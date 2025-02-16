{{ config(materialized='view') }}

SELECT
    date,
    "open",
    high,
    low,
    close,
    volume,
    price_change
FROM
    {{ source('public', 'cleaned_stock_data') }}