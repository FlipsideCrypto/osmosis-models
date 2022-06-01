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
    voter, 
    proposal_id, 
    CASE 
        WHEN vote_option = 1 THEN 
            'YES'
        WHEN vote_option = 2 THEN
            'ABSTAIN'
        WHEN vote_option = 3 THEN
            'NO'
        WHEN vote_option = 4 THEN
            'NO WITH VETO'
        ELSE 'NULL'
    END AS vote_option, 
    vote_weight
FROM {{ ref('silver__governance_votes') }}