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
    voter, 
    proposal_id, 
    vote_option, 
    vote_weight
FROM {{ ref('silver__governance_votes') }}