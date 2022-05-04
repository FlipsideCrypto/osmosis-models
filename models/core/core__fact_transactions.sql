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
    tx_code, 
    msgs

FROM {{ ref('silver__transactions') }} t

