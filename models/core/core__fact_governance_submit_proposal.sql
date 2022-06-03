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
    proposer, 
    proposal_id, 
    proposal_type
FROM {{ ref('silver__governance_submit_proposal') }}