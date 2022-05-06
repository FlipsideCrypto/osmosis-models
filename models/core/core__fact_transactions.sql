{{ config(
    materialized = 'view'
) }}

WITH fees AS (
    SELECT 
        tx_id, 
        COALESCE(attribute_value, 
                 '0uosmo')
        AS fee
    FROM  {{ ref('silver__msg_attributes') }}
  
    WHERE attribute_key = 'fee'
)

SELECT 
    t.block_id, 
    t.block_timestamp, 
    t.blockchain, 
    t.chain_id, 
    t.tx_id,
    s.attribute_value AS tx_from, 
    tx_status, 
    codespace, 
    f.fee, 
    gas_used, 
    gas_wanted,
    tx_code, 
    msgs
FROM {{ ref('silver__transactions') }} t

INNER JOIN fees f 
ON t.tx_id = f.tx_id

INNER JOIN {{ ref('silver__msg_attributes') }} s
ON t.tx_id = s.tx_id

WHERE s.attribute_key = 'acc_seq'

