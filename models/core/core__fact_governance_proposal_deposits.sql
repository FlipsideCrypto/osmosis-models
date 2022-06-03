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
    depositor, 
    proposal_id, 
    amount, 
    currency, 
    decimal
FROM {{ ref('silver__governance_proposal_deposits') }}