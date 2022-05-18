{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, sender, receiver)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH message_index AS (
    SELECT 
        tx_id, 
        attribute_key, 
        MIN(msg_index) as min_index
    FROM 
        {{ ref('silver__msg_attributes') }}
    WHERE
        msg_type = 'coin_spent'
    AND 
        attribute_key = 'amount'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
  
    GROUP BY 
        tx_id, 
        attribute_key
),  

coin_sent AS (
    SELECT 
        a.tx_id, 
         SPLIT_PART(
            TRIM(
                REGEXP_REPLACE(
                    attribute_value,
                    '[^[:digit:]]',
                    ' '
                )
            ),
            ' ',
            0
        ) AS amount_sent,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS currency_sent,
        l.raw_metadata [1] :exponent AS currency_sent_decimal
  
        FROM {{ ref('silver__msg_attributes') }} a 
  
        LEFT OUTER JOIN message_index m
        ON a.tx_id = m.tx_id 
        AND a.attribute_key = m.attribute_key
  
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} l
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
  
        WHERE msg_type = 'coin_spent' 
        AND a.msg_index = m.min_index

        {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
        {% endif %}
        
), 

sender AS (
    SELECT
        tx_id,
        SPLIT_PART(
            attribute_value,
            '/',
            0
        ) AS sender
    FROM
        {{ ref('silver__msg_attributes') }}
    WHERE
        attribute_key = 'acc_seq'
    
    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

receiver_ibc AS (
    SELECT 
        tx_id, 
        attribute_value AS receiver
    FROM 
        {{ ref('silver__msg_attributes') }}
    WHERE 
        msg_type = 'ibc_transfer'
    AND 
        attribute_key = 'receiver'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

osmo_tx_ids AS (
    SELECT
        tx_id
    FROM 
        {{ ref('silver__msg_attributes') }}
    WHERE 
        msg_type = 'message' 
    AND 
        attribute_key = 'module' 
    AND 
        attribute_value = 'bank'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

osmo_receiver AS (
    SELECT 
    DISTINCT
        o.tx_id, 
        attribute_value as receiver
    FROM osmo_tx_ids o 
  
    LEFT OUTER JOIN {{ ref('silver__msg_attributes') }} m 
    ON o.tx_id = m.tx_id 
  
    WHERE 
        m.msg_type = 'coin_received'
    AND 
        m.attribute_key = 'receiver'  
    
    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
)

SELECT 
    block_id, 
    block_timestamp, 
    blockchain, 
    chain_id, 
    r.tx_id, 
    sender, 
    amount_sent, 
    currency_sent,
    currency_sent_decimal, 
    receiver, 
    _ingested_at
FROM receiver_ibc r

LEFT OUTER JOIN coin_sent c
ON r.tx_id = c.tx_id

LEFT OUTER JOIN sender s
ON r.tx_id = s.tx_id 

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON r.tx_id = t.tx_id 

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}

UNION ALL 

SELECT 
    block_id, 
    block_timestamp, 
    blockchain, 
    chain_id, 
    r.tx_id, 
    sender, 
    amount_sent, 
    currency_sent,
    currency_sent_decimal, 
    receiver, 
    _ingested_at
FROM osmo_receiver r

LEFT OUTER JOIN coin_sent c
ON r.tx_id = c.tx_id

LEFT OUTER JOIN sender s
ON r.tx_id = s.tx_id 

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON r.tx_id = t.tx_id 

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}