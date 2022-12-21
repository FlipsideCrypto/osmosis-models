{{ config(
    materialized = 'view'
) }}

SELECT 
    block_id, 
    block_timestamp, 
    tx_id, 
    tx_succeeded,
    proposer, 
    proposal_id, 
    proposal_type
FROM {{ ref('silver__governance_submit_proposal') }}