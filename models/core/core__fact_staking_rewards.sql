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
    tx_caller_address, 
    action, 
    delegator_address,
    validator_address,  
    amount, 
    currency, 
    decimal
FROM {{ ref('silver__staking_rewards') }}