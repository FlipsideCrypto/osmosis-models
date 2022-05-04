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
    msg_index, 
    msg_type, 
    msg
FROM 
    {{ ('silver__msgs') }}