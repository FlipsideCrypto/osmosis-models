{{ config(
    materialized = 'view'
) }}

SELECT
    block_id, 
    block_timestamp, 
    tx_id, 
    tx_succeeded, 
    voter, 
    proposal_id, 
    vote_option, 
    vote_weight
FROM {{ ref('silver__governance_votes') }}