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
    delegator_address, 
    amount, 
    currency, 
    decimal, 
    validator_address, 
    lock_id, 
    original_superfluid_delegate_tx_ID
FROM 
    {{ ref('silver__superfluid_staking') }}