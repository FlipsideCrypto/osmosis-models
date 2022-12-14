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
    tx_succeeded,
    transfer_type, 
    sender, 
    amount, 
    currency, 
    decimal, 
    receiver
FROM {{ ref('silver__airdrops') }}