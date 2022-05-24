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
    msg_group,
    msg_index, 
    msg_type, 
    msg
FROM 
    {{ ref('silver__msgs') }}