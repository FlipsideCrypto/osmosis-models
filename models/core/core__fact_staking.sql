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
    tx_caller_address, 
    action, 
    msg_group, 
    delegator_address,
    validator_address, 
    redelegate_source_validator_address,  
    amount, 
    currency, 
    decimal, 
    completion_time
FROM {{ ref('silver__staking') }}