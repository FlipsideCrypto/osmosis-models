{{ config(
    materialized = 'view'
) }}

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    tx_succeeded,
    depositor, 
    proposal_id, 
    amount, 
    currency, 
    decimal
FROM {{ ref('silver__governance_proposal_deposits') }}