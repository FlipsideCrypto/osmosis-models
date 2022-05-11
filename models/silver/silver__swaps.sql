{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH message_indexes AS (
    SELECT
        tx_id,
        attribute_key, 
        min(msg_index) as min_index,  
        max(msg_index) as max_index
    FROM {{ ref('silver__msg_attributes') }}
  
    WHERE msg_type = 'token_swapped'
    AND (attribute_key = 'tokens_in' OR attribute_key = 'tokens_out') 
    {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
    GROUP BY tx_id, attribute_key

),   

tokens_in AS (
    SELECT 
        t.tx_id, 
        split_part(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0) / POW(10, COALESCE(l.raw_metadata[1]:exponent, 0)) AS swap_from_amount, 
        right(attribute_value, length(attribute_value) - length(split_part(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS swap_from_currency
    FROM {{ ref('silver__msg_attributes') }} t
  
    LEFT OUTER JOIN message_indexes m 
    ON t.tx_id = m.tx_id and t.attribute_key = m.attribute_key
  
    INNER JOIN {{ ref('silver__asset_metadata') }} l 
    ON right(attribute_value, length(attribute_value) - length(split_part(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
  
    WHERE msg_type = 'token_swapped'
    AND t.attribute_key = 'tokens_in'
    AND t.msg_index = m.min_index
    {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
),  

tokens_out AS (
    SELECT 
        t.tx_id, 
        split_part(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0) / POW(10, COALESCE(l.raw_metadata[1]:exponent, 0)) AS swap_to_amount, 
        right(attribute_value, length(attribute_value) - length(split_part(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS swap_to_currency
    FROM {{ ref('silver__msg_attributes') }} t
    
    LEFT OUTER JOIN message_indexes m 
    ON t.tx_id = m.tx_id and t.attribute_key = m.attribute_key
  
    INNER JOIN {{ ref('silver__asset_metadata') }} l 
    ON right(attribute_value, length(attribute_value) - length(split_part(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
  
    WHERE msg_type = 'token_swapped'
    AND t.attribute_key = 'tokens_out'
    AND t.msg_index = m.max_index

    {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
),  

pools AS (
    SELECT
        tx_id, 
        array_agg(attribute_value :: INTEGER) as pool_ids
    FROM {{ ref('silver__msg_attributes') }}
    WHERE attribute_key = 'pool_id'

    {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
   
    GROUP BY tx_id 
), 

trader AS (
    SELECT 
        tx_id, 
        split_part(attribute_value, '/', 0) as trader
    FROM {{ ref('silver__msg_attributes') }} 
    WHERE attribute_key = 'acc_seq'

    {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
    
)

SELECT 
   t.block_id, 
   t.block_timestamp, 
   t.blockchain, 
   t.chain_id, 
   t.tx_id, 
   t.tx_status,
   s.trader, 
   f.swap_from_amount AS swap_from_amount, 
   f.swap_from_currency,  
   tt.swap_to_amount, 
   tt.swap_to_currency, 
   pool_ids, 
   t._ingested_at

FROM {{ ref('silver__transactions') }} t 

INNER JOIN tokens_in f 
ON t.tx_id = f.tx_id

INNER JOIN tokens_out tt
ON f.tx_id = tt.tx_id 

INNER JOIN trader s
ON t.tx_id = s.tx_id

INNER JOIN pools p
ON t.tx_id = p.tx_id 

{% if is_incremental() %}
    WHERE t._ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}