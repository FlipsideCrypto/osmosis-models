{{ config(
    materialized = 'view'
) }}

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    tx_succeeded, 
    trader, 
    from_amount, 
    from_currency, 
    from_decimal, 
    to_amount, 
    to_currency, 
    to_decimal, 
    pool_ids
FROM {{ ref('silver__swaps') }}