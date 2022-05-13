{{ config(
  materialized = 'incremental',
  unique_key = "tx_id",
  incremental_strategy = 'delete+insert',
  cluster_by = ['_ingested_at::DATE'],
) }}

WITH proposal_ids AS (
    SELECT 
        tx_id, 
        attribute_value AS proposal_id 
    FROM {{ ref('silver__msg_attributes') }}
    WHERE msg_type = 'proposal_deposit' 
    AND attribute_key = 'proposal_id'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

deposit_value AS (
    SELECT 
        tx_id, 
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
        ) / POW(10, COALESCE(raw_metadata[1]:exponent, 0)) AS deposit_amount, 
        
        RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) AS deposit_currency
  
        FROM {{ ref('silver__msg_attributes') }} m
  
        LEFT OUTER JOIN {{ ref('silver__asset_metadata') }} a
        ON RIGHT(attribute_value, LENGTH(attribute_value) - LENGTH(SPLIT_PART(TRIM(REGEXP_REPLACE(attribute_value, '[^[:digit:]]', ' ')), ' ', 0))) = a.address
  
        WHERE msg_type = 'proposal_deposit' 
        AND attribute_key = 'amount'

        AND attribute_value IS NOT NULL

        {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
        {% endif %}
        
), 

depositors AS (
    SELECT 
        tx_id, 
        split_part(attribute_value, '/', 0) as depositor 
    FROM {{ ref('silver__msg_attributes') }}
    WHERE attribute_key = 'acc_seq'

    {% if is_incremental() %}
    AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
)

SELECT 
   block_id, 
   block_timestamp, 
   blockchain, 
   chain_id, 
   p.tx_id, 
   tx_status,
   d.depositor, 
   p.proposal_id,
   v.deposit_amount, 
   v.deposit_currency, 
   _ingested_at
FROM deposit_value v

INNER JOIN proposal_ids p 
ON p.tx_id = v.tx_id 

INNER JOIN depositors d
ON v.tx_id = d.tx_id 

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON v.tx_id = t.tx_id

{% if is_incremental() %}
WHERE
    t._ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}