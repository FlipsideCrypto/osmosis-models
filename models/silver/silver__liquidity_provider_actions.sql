{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, msg_index)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH message_indexes AS (

    SELECT
        tx_id,
        attribute_key,
        msg_index
    FROM
        {{ ref('silver__msg_attributes') }}
     WHERE 
        (msg_type = 'pool_exited' 
        OR msg_type = 'pool_joined')
    AND (attribute_key = 'tokens_in'
        OR attribute_key = 'tokens_out')

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}

),

pool_ids AS (
    SELECT 
        a.tx_id, 
        a.msg_index, 
        attribute_value :: INTEGER AS pool_id
        
    FROM 
        {{ ref('silver__msg_attributes') }} a

    LEFT OUTER JOIN message_indexes m 
    ON a.tx_id = m.tx_id 
    
    WHERE 
        (msg_type = 'pool_exited' 
        OR msg_type = 'pool_joined')
    AND 
        a.attribute_key = 'pool_id'
    AND 
        a.msg_index = m.msg_index

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
  
), 

token_array AS ( 
    SELECT 
        a.tx_id, 
        a.msg_index, 
        msg_type AS action, 
        split(attribute_value, ',') AS tokens
    FROM 
        {{ ref('silver__msg_attributes') }} a
  
     LEFT OUTER JOIN message_indexes m
        ON a.tx_id = m.tx_id
        AND a.attribute_key = m.attribute_key
    
    WHERE 
       (msg_type = 'pool_exited' 
        OR msg_type = 'pool_joined')
    AND 
       (a.attribute_key = 'tokens_in'
        OR a.attribute_key = 'tokens_out')
  
    AND a.msg_index = m.msg_index
  
    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
  
), 

tokens AS (
     SELECT 
        tx_id,
        msg_index,
        action, 
        SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    t.value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0)::INTEGER AS amount, 
            RIGHT(t.value, LENGTH(t.value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(t.value, '[^[:digit:]]', ' ')), ' ', 0)))::STRING AS currency 
    FROM token_array, 
    LATERAL FLATTEN (input => tokens) t
), 

decimals AS (
      SELECT 
        tx_id, 
        msg_index,
        action, 
        amount, 
        currency, 
        raw_metadata [1] :exponent AS decimal
    FROM tokens t
    
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }}
    ON currency = address
),  

lper AS (
    SELECT 
        tx_id, 
        split_part(attribute_value, '/', 0) as liquidity_provider_address
    FROM {{ ref('silver__msg_attributes') }}
    WHERE attribute_key = 'acc_seq'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
)

SELECT 
    tx.block_id, 
    tx.block_timestamp, 
    tx.blockchain, 
    tx.chain_id, 
    d.tx_id, 
    tx_status, 
    d.msg_index, 
    liquidity_provider_address, 
    action,
    pool_id, 
    amount, 
    currency, 
    decimal, 
    _ingested_at
FROM decimals d

LEFT OUTER JOIN pool_ids p
ON d.tx_id = p.tx_id
AND d.msg_index = p.msg_index

LEFT OUTER JOIN lper l
ON d.tx_id = l.tx_id 
    
LEFT OUTER JOIN {{ ref('silver__transactions') }} tx
ON d.tx_id = tx.tx_id


{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}



