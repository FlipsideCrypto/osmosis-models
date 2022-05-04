{{ config(
    materialized = 'view'
) }}

SELECT 
    block_id, 
    block_timestamp, 
    chain_id, 
    tx_count, 
    proposer_address, 
    validator_hash
FROM 
    {{ ref('silver__blocks') }}