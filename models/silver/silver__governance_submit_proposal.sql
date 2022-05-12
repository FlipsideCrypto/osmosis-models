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
  WHERE msg_type = 'submit_proposal'
  AND attribute_key = 'proposal_id'

  {% if is_incremental() %}
       AND _ingested_at :: DATE >= CURRENT_DATE - 2
  {% endif %}
), 

proposal_type AS (
    SELECT 
        tx_id, 
        attribute_value AS proposal_type
    FROM {{ ref('silver__msg_attributes') }}
    WHERE msg_type = 'submit_proposal'
    AND attribute_key = 'proposal_type'

    {% if is_incremental() %}
        AND _ingested_at :: DATE >= CURRENT_DATE - 2
    {% endif %}
), 

proposer AS (
    SELECT 
        tx_id, 
        split_part(attribute_value, '/', 0) as proposer
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
   proposer, 
   p.proposal_id,
   y.proposal_type, 
   _ingested_at
FROM proposal_ids p

INNER JOIN proposal_type y
ON p.tx_id = y.tx_id 

INNER JOIN proposer pp 
ON p.tx_id = pp.tx_id 

LEFT OUTER JOIN {{ ref('silver__transactions') }} t
ON p.tx_id = t.tx_id

{% if is_incremental() %}
    WHERE t._ingested_at :: DATE >= CURRENT_DATE - 2
{% endif %}