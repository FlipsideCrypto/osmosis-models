{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert', 
  cluster_by = ['block_timestamp::DATE'],
) }}

WITH fee AS (
    SELECT 
        tx_id, 
        attribute_value AS fee
        
    FROM  {{ ref('silver__msg_attributes') }}
    WHERE attribute_key = 'fee'

    {% if is_incremental() %}
        AND
        _ingested_at :: DATE >= CURRENT_DATE -2
    {% endif %}

), 

spender AS (
   SELECT 
        tx_id, 
        split_part(attribute_value, '/', 0) AS tx_from
        
    FROM  {{ ref('silver__msg_attributes') }}
    WHERE attribute_key = 'acc_seq'

    {% if is_incremental() %}
        AND
        _ingested_at :: DATE >= CURRENT_DATE -2
    {% endif %} 
)

SELECT 
    t.block_id, 
    t.block_timestamp, 
    t.blockchain, 
    t.chain_id, 
    t.tx_id,
    s.tx_from, 
    tx_status, 
    codespace, 
    COALESCE( fee,
            '0uosmo') AS fee,  
    gas_used, 
    gas_wanted,
    tx_code, 
    msgs
FROM {{ ref('silver__transactions') }} t

LEFT OUTER JOIN fee f 
ON t.tx_id = f.tx_id

LEFT OUTER JOIN spender s
ON t.tx_id = s.tx_id

{% if is_incremental() %}
AND
  _ingested_at :: DATE >= CURRENT_DATE -2
{% endif %}

