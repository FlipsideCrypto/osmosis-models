{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'delete+insert',
    cluster_by = ['block_timestamp::DATE'],
) }}

WITH early_swaps AS (
    SELECT 
        DISTINCT tx_id
    FROM {{ ref('silver__msg_attributes') }}
    WHERE 
        block_timestamp :: date < '2021-09-25'
    AND 
        msg_type = 'message'
    AND 
        attribute_key = 'action'
    AND 
        attribute_value = 'swap_exact_amount_in'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
),  

indexes AS (
    SELECT 
        e.tx_id, 
        MIN(m.msg_index) AS index_in, 
        MAX(m.msg_index) AS index_out
    FROM early_swaps e
    
    LEFT OUTER JOIN {{ ref('silver__msg_attributes') }} m
    ON e.tx_id = m.tx_id 
  
    WHERE m.msg_type = 'transfer'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
  
    GROUP BY e.tx_id
), 

trader AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS trader
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        block_timestamp :: date < '2021-09-25'
    AND 
        attribute_key = 'acc_seq'
    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

from_token AS (
    SELECT 
        i.tx_id, 
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS from_currency, 

        SPLIT_PART(TRIM(REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' ')), ' ', 0) AS from_amount, 
        a.raw_metadata [1] :exponent AS from_decimal
  
    FROM indexes i 
    
    LEFT OUTER JOIN {{ ref('silver__msg_attributes') }} m 
    ON i.tx_id = m.tx_id 
    AND i.index_in = m.msg_index
  
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
    ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = a.address
  
    WHERE 
        msg_type = 'transfer'
    AND 
        attribute_key = 'amount'
    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

to_token AS (
    SELECT 
        i.tx_id, 
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS to_currency, 

        SPLIT_PART(TRIM(REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' ')), ' ', 0) AS to_amount, 
        a.raw_metadata [1] :exponent AS to_decimal
  
    FROM indexes i 
    
    LEFT OUTER JOIN {{ ref('silver__msg_attributes') }} m 
    ON i.tx_id = m.tx_id 
    AND i.index_out = m.msg_index
  
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
    ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = a.address
  
    WHERE 
        msg_type = 'transfer'
    AND 
        attribute_key = 'amount'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
) 

SELECT 
    block_id, 
    block_timestamp, 
    blockchain, 
    chain_id, 
    e.tx_id, 
    tx_status, 
    tt.trader, 
    from_amount, 
    from_currency, 
    CASE
        WHEN f.from_currency LIKE 'gamm/pool/%' THEN 18
        ELSE f.from_decimal
    END AS from_decimal, 
    to_amount, 
    to_currency, 
    CASE
        WHEN tok.to_currency LIKE 'gamm/pool/%' THEN 18
        ELSE tok.to_decimal
    END AS to_decimal, 
    NULL as pool_ids, 
    _ingested_at
FROM early_swaps e

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON e.tx_id = t.tx_id 

INNER JOIN trader tt
ON e.tx_id = tt.tx_id 

INNER JOIN from_token f
ON e.tx_id = f.tx_id 

INNER JOIN to_token tok
ON e.tx_id = tok.tx_id 

{% if is_incremental() %}
WHERE _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}