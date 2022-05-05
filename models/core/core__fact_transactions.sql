{{ config(
    materialized = 'view'
) }}

WITH base_table AS (
    SELECT
        block_id, 
        block_timestamp, 
        blockchain, 
        chain_id, 
        tx_id, 
        tx_status, 
        codespace, 
        gas_used, 
        gas_wanted,
        tx_code, 
        msgs

    FROM {{ ref('silver__transactions') }}
), 

fee AS (
    SELECT 
        tx_id, 
        COALESCE(attribute_value, 
                 '0uosmo')
        AS fee
    FROM  {{ ref('silver__msg_attributes') }}
  
    WHERE attribute_key = 'fee'
), 

sender AS (
    SELECT 
        tx_id, 
        attribute_value AS sender
    FROM  {{ ref('silver__msg_attributes') }}
  
    WHERE attribute_key = 'acc_seq'
),

receiver AS (
    SELECT 
        tx_id, 
        array_agg(attribute_value) AS receiver
    FROM  {{ ref('silver__msg_attributes') }}
  
    WHERE attribute_key = 'receiver'
    GROUP BY tx_id
)

SELECT 
    block_id, 
    block_timestamp, 
    blockchain, 
    chain_id, 
    b.tx_id,
    s.sender, 
    tx_status, 
    codespace, 
    f.fee
    gas_used, 
    gas_wanted,
    tx_code, 
    msgs
FROM base_table b 

INNER JOIN sender s
ON b.tx_id = s.tx_id

INNER JOIN fee f 
ON b.tx_id = f.tx_id

