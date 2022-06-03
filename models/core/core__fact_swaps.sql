{{ config(
    materialized = 'view'
) }}

SELECT 
    block_id, 
    block_timestamp, 
    blockchain, 
    chain_id, 
    tx_id, 
    tx_status, 
    trader, 
    from_amount, 
    from_currency, 
    from_decimal, 
    to_amount, 
    to_currency, 
    to_decimal, 
    pool_ids
FROM {{ ref('silver__swaps') }}