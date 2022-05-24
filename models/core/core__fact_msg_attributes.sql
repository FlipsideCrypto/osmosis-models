{{ config(
    materialized = 'view'
) }}

SELECT
    block_id, 
    block_timestamp, 
    blockchain, 
    chain_id, 
    tx_id, 
    msg_group,
    msg_index, 
    msg_type, 
    attribute_index, 
    attribute_key, 
    attribute_value 
FROM 
    {{ ref('silver__msg_attributes') }}