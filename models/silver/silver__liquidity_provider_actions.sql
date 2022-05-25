{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH message_indexes AS (

    SELECT
        tx_id,
        attribute_key,
        MIN(msg_index) AS min_index
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

    GROUP BY
        tx_id,
        attribute_key
),

pool_ids AS (
    SELECT 
        tx_id, 
        ARRAY_AGG(
        attribute_value :: INTEGER
        ) AS pool_id
    FROM 
        {{ ref('silver__msg_attributes') }}
    
    WHERE 
        (msg_type = 'pool_exited' 
        OR msg_type = 'pool_joined')
    AND attribute_key = 'pool_id'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
  
  GROUP BY tx_id
), 

token_array AS ( 
    SELECT 
        a.tx_id, 
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
  
    AND a.msg_index = m.min_index
  
    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
  
), 

tokens AS (
    SELECT 
        tx_id, 
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
            0) AS amount, 
            RIGHT(t.value, LENGTH(t.value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(t.value, '[^[:digit:]]', ' ')), ' ', 0))) AS currency
    FROM token_array, 
    LATERAL FLATTEN (input => tokens) t
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
    liquidity_provider_address, 
    action,
    pool_id, 
    amount, 
    currency, 
    raw_metadata[1]:exponent AS decimal, 
    _ingested_at
FROM pool_ids p

LEFT OUTER JOIN tokens t
ON p.tx_id = t.tx_id

LEFT OUTER JOIN lper l
ON p.tx_id = l.tx_id 
    
LEFT OUTER JOIN {{ ref('silver__transactions') }} tx
ON p.tx_id = tx.tx_id

LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
ON currency = a.address

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}



