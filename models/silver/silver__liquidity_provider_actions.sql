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
        {{ ref('silver__msg_attributes') }}
    
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
  
  GROUP BY tx_id
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
    
    LEFT OUTER JOIN "OSMOSIS_DEV"."SILVER"."ASSET_METADATA" 
    ON currency = address
),  

tokens_decimals AS (
    SELECT 
        tx_id, 
        msg_index, 
        action, 
        ARRAY_AGG(amount) AS amount, 
        ARRAY_AGG(currency) AS currency, 
        ARRAY_AGG(decimal) AS decimal
    FROM decimals
    
    GROUP BY tx_id, msg_index, action
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
    block_id, 
    block_timestamp, 
    tx.blockchain, 
    chain_id, 
    p.tx_id, 
    tx_status, 
    t.msg_index, 
    liquidity_provider_address, 
    action,
    pool_id, 
    amount, 
    currency, 
    decimal, 
    _ingested_at
FROM pool_ids p

LEFT OUTER JOIN tokens_decimals t
ON p.tx_id = t.tx_id
AND p.msg_index = t.msg_index

LEFT OUTER JOIN lper l
ON p.tx_id = l.tx_id 
    
LEFT OUTER JOIN {{ ref('silver__transactions') }} tx
ON p.tx_id = tx.tx_id

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}



