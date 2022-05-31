{{ config(
  materialized = 'incremental',
  unique_key = "CONCAT_WS('-', tx_id, msg_index, currency)",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH sender AS (
    SELECT
        tx_id, 
        msg_index, 
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

message_index_ibc AS (
    SELECT 
        att.tx_id, 
        MAX(att.msg_index) as max_index
    FROM 
        {{ ref('silver__msg_attributes') }} att

    INNER JOIN sender s
    ON att.tx_id = s.tx_id

    WHERE
        msg_type = 'coin_spent' OR msg_type = 'transfer' 
    AND 
        attribute_key = 'amount'
    AND 
        att.msg_index > s.msg_index 

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
  
    GROUP BY 
        att.tx_id
),  

coin_sent_ibc AS (
    SELECT 
        a.tx_id, 
         COALESCE(
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
            ), 
            TRY_PARSE_JSON(attribute_value):amount
          ) AS amount,
        COALESCE(
            RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))), 
            TRY_PARSE_JSON(attribute_value)[1]:denom )
            AS currency,
        l.raw_metadata [1] :exponent AS decimal
  
        FROM {{ ref('silver__msg_attributes') }} a 
  
        LEFT OUTER JOIN message_index_ibc m
        ON a.tx_id = m.tx_id 
  
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} l
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
  
        WHERE a.msg_index = m.max_index
        AND a.attribute_key = 'amount'

        {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
        {% endif %}
        
), 

receiver_ibc AS (
    SELECT 
        tx_id, 
        COALESCE(
            attribute_value, 
            TRY_PARSE_JSON(attribute_value):receiver
        ) AS receiver, 
        MAX(msg_index) AS msg_index
    FROM 
        {{ ref('silver__msg_attributes') }}
     WHERE 
        msg_type = 'ibc_transfer'
    AND 
        attribute_key = 'receiver'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}

     GROUP BY tx_id, receiver
), 

osmo_tx_ids AS (
    SELECT
        DISTINCT tx_id
    FROM {{ ref('silver__msg_attributes') }}
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

message_indexes_osmo AS (
    SELECT 
        v.tx_id, 
        attribute_key, 
        m.msg_index
    FROM 
       osmo_tx_ids v
  
    LEFT OUTER JOIN {{ ref('silver__msg_attributes') }} m 
    ON v.tx_id = m.tx_id 

    INNER JOIN sender s
    ON v.tx_id = s.tx_id
    
    WHERE
        msg_type = 'transfer'
    AND 
        attribute_key = 'amount'
    AND 
        m.msg_index > s.msg_index 

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
  
),  

osmo_receiver AS (
    SELECT 
        o.tx_id,
        m.msg_index,
        attribute_value as receiver
    FROM osmo_tx_ids o 
  
    LEFT OUTER JOIN {{ ref('silver__msg_attributes') }} m 
    ON o.tx_id = m.tx_id 
  
    LEFT OUTER JOIN message_indexes_osmo idx
    ON idx.tx_id = m.tx_id 
    
  
    WHERE 
        m.msg_type = 'transfer'
    AND 
        m.attribute_key = 'recipient' 
    AND 
        idx.msg_index = m.msg_index
    
    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
),

osmo_amount AS (
    SELECT 
        o.tx_id,
        m.msg_index,
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
        ) AS amount,
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS currency,
        l.raw_metadata [1] :exponent AS decimal
    FROM osmo_tx_ids o 
  
    LEFT OUTER JOIN {{ ref('silver__msg_attributes') }} m 
    ON o.tx_id = m.tx_id 
  
    LEFT OUTER JOIN message_indexes_osmo idx
    ON idx.tx_id = m.tx_id 
  
    LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} l
    ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = l.address
  
    WHERE 
        m.msg_type = 'transfer'
    AND 
        m.attribute_key = 'amount' 
    AND 
        idx.msg_index = m.msg_index 
    
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
    tx_status, 
    'IBC_TRANSFER_OUT' AS transfer_type,  
    r.msg_index, 
    sender, 
    amount, 
    currency,
    decimal, 
    receiver, 
    _ingested_at
FROM receiver_ibc r

LEFT OUTER JOIN coin_sent_ibc c
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
    tx_status, 
    'OSMOSIS' AS transfer_type, 
    r.msg_index, 
    sender, 
    amount, 
    currency,
    decimal, 
    receiver, 
    _ingested_at
FROM osmo_receiver r

LEFT OUTER JOIN osmo_amount c
ON r.tx_id = c.tx_id
AND r.msg_index = c.msg_index

LEFT OUTER JOIN sender s
ON r.tx_id = s.tx_id 

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON r.tx_id = t.tx_id 

{% if is_incremental() %}
AND _ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}

UNION ALL 

SELECT
    m.block_id, 
    m.block_timestamp, 
    m.blockchain, 
    m.chain_id, 
    s.tx_id, 
    tx_status,
    'IBC_TRANSFER_IN' AS transfer_type,  
    m.msg_index,
    TRY_PARSE_JSON(attribute_value):sender :: STRING AS sender, 
    TRY_PARSE_JSON(attribute_value):amount :: INTEGER AS amount, 
    TRY_PARSE_JSON(attribute_value):denom :: STRING AS currency,
    raw_metadata[1]:exponent :: INTEGER AS decimal, 
    TRY_PARSE_JSON(attribute_value):receiver :: STRING AS receiver, 
    m._ingested_at
    
FROM sender s

LEFT OUTER JOIN {{ ref('silver__msg_attributes') }} m 
ON s.tx_id = m.tx_id 

LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
ON TRY_PARSE_JSON(attribute_value):denom :: STRING = COALESCE(raw_metadata[0]:aliases[0] :: STRING, raw_metadata[0]:denom :: STRING)

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON s.tx_id = t.tx_id 

WHERE m.msg_type = 'write_acknowledgement' 
AND m.attribute_key = 'packet_data'

{% if is_incremental() %}
AND m._ingested_at :: DATE >= CURRENT_DATE - 2
AND t._ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}