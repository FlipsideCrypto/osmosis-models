{{ config(
    materialized = 'view'
) }}

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    tx_succeeded,
    transfer_type, 
    sender, 
    amount, 
    currency, 
    decimal, 
    receiver
FROM {{ ref('silver__airdrops') }}