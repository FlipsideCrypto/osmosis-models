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
    msg_index, 
    liquidity_provider_address, 
    action, 
    pool_id, 
    amount, 
    currency, 
    decimal
FROM {{ ref('silver__liquidity_provider_actions') }}